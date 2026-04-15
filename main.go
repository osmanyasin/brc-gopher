package main

import (
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
	min, max, sum int64
	count         int64
}

func (s *Stats) update(temp int64) {
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
	return float64(s.sum) / float64(s.count) / 10.0
}

func (s *Stats) minF() float64 { return float64(s.min) / 10.0 }
func (s *Stats) maxF() float64 { return float64(s.max) / 10.0 }

func parseTemp(data []byte, pos int) (int64, int) {
	negative := data[pos] == '-'
	if negative {
		pos++
	}

	var temp int64
	for pos < len(data) {
		c := data[pos]
		pos++
		if c == '\n' {
			break
		}
		if c != '.' && c != '\r' {
			temp = temp*10 + int64(c-'0')
		}
	}

	if negative {
		return -temp, pos
	}
	return temp, pos
}

func processChunk(data []byte) map[string]*Stats {
	results := make(map[string]*Stats)
	pos := 0

	for pos < len(data) {
		start := pos

		for pos < len(data) && data[pos] != ';' {
			pos++
		}
		if pos >= len(data) {
			break
		}

		station := string(data[start:pos])
		pos++ // skip ';'

		if pos >= len(data) {
			break
		}

		temp, nextPos := parseTemp(data, pos)
		pos = nextPos

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
		fmt.Printf("%s=%.1f/%.1f/%.1f", s, res.minF(), res.mean(), res.maxF())
	}
	fmt.Println("}")
	fmt.Printf("Elapsed: %s\n", time.Since(start))
}
