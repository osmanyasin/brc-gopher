package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
)

type Stats struct {
	min, max, sum float64
	count         int64
}

func (s *Stats) update(temp float64) {
	if temp < s.min {
		s.min = temp
	}
	if temp > s.max {
		s.max = temp
	}
	s.sum += temp
	s.count++
}

func (s *Stats) merge(other *Stats) {
	if other.min < s.min {
		s.min = other.min
	}
	if other.max > s.max {
		s.max = other.max
	}
	s.sum += other.sum
	s.count += other.count
}

// parseFloat ignores '\r' and whitespace to handle all line endings
func parseFloat(b []byte) (float64, error) {
	b = bytes.TrimSpace(b) // Remove \r, \n, and spaces
	if len(b) == 0 {
		return 0, io.EOF
	}

	negative := false
	if b[0] == '-' {
		negative = true
		b = b[1:]
	}

	var result float64
	var decimal bool
	var decimalPlace float64 = 0.1

	for _, c := range b {
		if c == '.' {
			decimal = true
			continue
		}
		if c < '0' || c > '9' {
			continue
		} // Skip non-numeric chars like \r

		digit := float64(c - '0')
		if decimal {
			result += digit * decimalPlace
			decimalPlace *= 0.1
		} else {
			result = result*10 + digit
		}
	}
	if negative {
		result = -result
	}
	return result, nil
}

func processChunk(data []byte) map[string]*Stats {
	results := make(map[string]*Stats)
	lines := bytes.Split(data, []byte("\n"))

	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		sepIdx := bytes.IndexByte(line, ';')
		if sepIdx == -1 {
			continue
		}

		station := string(line[:sepIdx])
		temp, err := parseFloat(line[sepIdx+1:])
		if err != nil {
			continue
		}

		if stats, exists := results[station]; exists {
			stats.update(temp)
		} else {
			results[station] = &Stats{min: temp, max: temp, sum: temp, count: 1}
		}
	}
	return results
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <file>")
		return
	}

	filename := os.Args[1]
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	fi, _ := file.Stat()
	fileSize := fi.Size()
	if fileSize == 0 {
		fmt.Println("{}")
		return
	}

	numWorkers := runtime.NumCPU()
	// For very small files, just use 1 worker
	if fileSize < 1024*1024 {
		numWorkers = 1
	}

	chunkSize := fileSize / int64(numWorkers)
	resultsChan := make(chan map[string]*Stats, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			f, _ := os.Open(filename)
			defer f.Close()

			start := int64(workerID) * chunkSize
			end := start + chunkSize
			if workerID == numWorkers-1 {
				end = fileSize
			}

			// 1. Align: If not at start, move to the first byte after the next '\n'
			if start > 0 {
				f.Seek(start-1, 0)
				b := make([]byte, 1)
				for {
					_, err := f.Read(b)
					if err != nil || b[0] == '\n' {
						break
					}
					start++
				}
			}

			// 2. Read until the end of this worker's chunk
			// AND continue until the very next newline to finish the line
			f.Seek(start, 0)
			length := end - start
			if length <= 0 && workerID != 0 {
				return
			}

			// Buffer to hold our chunk + a little extra for the rest of the last line
			data := make([]byte, length+1024)
			n, _ := io.ReadFull(f, data)
			data = data[:n]

			// If we aren't at the end of the file, we might have a partial line
			// Read until we hit a newline
			if end < fileSize {
				extra := make([]byte, 1)
				for {
					_, err := f.Read(extra)
					if err != nil {
						break
					}
					data = append(data, extra[0])
					if extra[0] == '\n' {
						break
					}
				}
			}

			resultsChan <- processChunk(data)
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	finalResults := make(map[string]*Stats)
	for chunkResults := range resultsChan {
		for station, stats := range chunkResults {
			if existing, exists := finalResults[station]; exists {
				existing.merge(stats)
			} else {
				finalResults[station] = stats
			}
		}
	}

	stations := make([]string, 0, len(finalResults))
	for s := range finalResults {
		stations = append(stations, s)
	}
	sort.Strings(stations)

	fmt.Print("{")
	for i, s := range stations {
		res := finalResults[s]
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f", s, res.min, res.sum/float64(res.count), res.max)
	}
	fmt.Println("}")
}
