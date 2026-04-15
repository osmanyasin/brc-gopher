package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	smallFileThreshold = 1024 * 1024
	maxLineLength      = 1024
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

func (s *Stats) mean() float64 {
	return s.sum / float64(s.count)
}

func parseTemp(raw []byte) (float64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, io.EOF
	}

	negative := raw[0] == '-'
	if negative {
		raw = raw[1:]
	}

	var integer, frac float64
	var seenDot bool
	var fracPlace float64 = 0.1

	for _, c := range raw {
		switch {
		case c == '.':
			seenDot = true
		case c >= '0' && c <= '9':
			digit := float64(c - '0')
			if seenDot {
				frac += digit * fracPlace
				fracPlace *= 0.1
			} else {
				integer = integer*10 + digit
			}
		}
	}

	result := integer + frac
	if negative {
		result = -result
	}
	return result, nil
}

func processChunk(data []byte) map[string]*Stats {
	results := make(map[string]*Stats)

	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		sepIdx := bytes.IndexByte(line, ';')
		if sepIdx == -1 {
			continue
		}

		station := string(line[:sepIdx])
		temp, err := parseTemp(line[sepIdx+1:])
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

func readChunk(filename string, workerID, numWorkers int, fileSize, chunkSize int64) map[string]*Stats {
	f, _ := os.Open(filename)
	defer f.Close()

	start := int64(workerID) * chunkSize
	end := start + chunkSize
	if workerID == numWorkers-1 {
		end = fileSize
	}

	if start > 0 {
		f.Seek(start-1, 0)
		buf := make([]byte, 1)
		for {
			_, err := f.Read(buf)
			if err != nil || buf[0] == '\n' {
				break
			}
			start++
		}
	}

	f.Seek(start, 0)
	length := end - start
	if length <= 0 && workerID != 0 {
		return nil
	}

	data := make([]byte, length+maxLineLength)
	n, _ := io.ReadFull(f, data)
	data = data[:n]

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

	return processChunk(data)
}

func mergeResults(dst, src map[string]*Stats) {
	for station, stats := range src {
		if existing, exists := dst[station]; exists {
			existing.merge(stats)
		} else {
			dst[station] = stats
		}
	}
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

	start := time.Now()

	numWorkers := runtime.NumCPU()
	if fileSize < smallFileThreshold {
		numWorkers = 1
	}

	chunkSize := fileSize / int64(numWorkers)
	resultsChan := make(chan map[string]*Stats, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			resultsChan <- readChunk(filename, workerID, numWorkers, fileSize, chunkSize)
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	finalResults := make(map[string]*Stats)
	for chunkResults := range resultsChan {
		mergeResults(finalResults, chunkResults)
	}

	stations := make([]string, 0, len(finalResults))
	for s := range finalResults {
		stations = append(stations, s)
	}
	sort.Strings(stations)

	fmt.Print("{")
	for i, s := range stations {
		if i > 0 {
			fmt.Print(", ")
		}
		res := finalResults[s]
		fmt.Printf("%s=%.1f/%.1f/%.1f", s, res.min, res.mean(), res.max)
	}
	fmt.Println("}")
	fmt.Printf("Elapsed: %s\n", time.Since(start))
}
