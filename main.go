package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const smallFileThreshold = 1024 * 1024

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

func (s *Stats) mean() float64 { return float64(s.sum) / float64(s.count) / 10.0 }
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
		pos++

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

func mmapFile(filename string) ([]byte, func(), error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	size := fi.Size()

	handle, err := windows.CreateFileMapping(
		windows.Handle(f.Fd()),
		nil,
		windows.PAGE_READONLY,
		0, 0,
		nil,
	)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("CreateFileMapping: %w", err)
	}

	addr, err := windows.MapViewOfFile(
		handle,
		windows.FILE_MAP_READ,
		0, 0,
		uintptr(size),
	)
	if err != nil {
		windows.CloseHandle(handle)
		f.Close()
		return nil, nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), size)

	cleanup := func() {
		windows.UnmapViewOfFile(addr)
		windows.CloseHandle(handle)
		f.Close()
	}
	return data, cleanup, nil
}

func findChunkBoundary(data []byte, pos int) int {
	for pos < len(data) && data[pos] != '\n' {
		pos++
	}
	if pos < len(data) {
		pos++
	}
	return pos
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

	data, cleanup, err := mmapFile(os.Args[1])
	if err != nil {
		fmt.Printf("Error mapping file: %v\n", err)
		return
	}
	defer cleanup()

	fileSize := len(data)
	if fileSize == 0 {
		fmt.Println("{}")
		return
	}

	startTime := time.Now()

	numWorkers := runtime.NumCPU()
	if fileSize < smallFileThreshold {
		numWorkers = 1
	}

	chunkSize := fileSize / numWorkers
	resultsChan := make(chan map[string]*Stats, numWorkers)
	var wg sync.WaitGroup

	start := 0
	for i := 0; i < numWorkers; i++ {
		end := start + chunkSize
		if i == numWorkers-1 {
			end = fileSize
		} else {
			end = findChunkBoundary(data, end)
		}

		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			resultsChan <- processChunk(chunk)
		}(data[start:end])

		start = end
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
	fmt.Printf("Elapsed: %s\n", time.Since(startTime))
}
