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

const (
	tableSize = 1 << 14
	tableMask = tableSize - 1
)

type Stats struct {
	min, max, sum int64
	count         int64
	name          []byte
}

type Table struct {
	entries []Stats
	used    []bool
}

func newTable() *Table {
	return &Table{
		entries: make([]Stats, tableSize),
		used:    make([]bool, tableSize),
	}
}

func hashBytes(b []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

func (t *Table) getOrCreate(name []byte) *Stats {
	h := hashBytes(name)
	idx := h & tableMask

	for t.used[idx] {
		if len(t.entries[idx].name) == len(name) {
			match := true
			for i := range name {
				if t.entries[idx].name[i] != name[i] {
					match = false
					break
				}
			}
			if match {
				return &t.entries[idx]
			}
		}
		idx = (idx + 1) & tableMask
	}

	t.used[idx] = true
	t.entries[idx] = Stats{
		name: name,
		min:  1000,
		max:  -1000,
	}
	return &t.entries[idx]
}

func parseTempFast(data []byte, pos int) (int64, int) {
	neg := false
	if data[pos] == '-' {
		neg = true
		pos++
	}

	var val int64
	for {
		c := data[pos]
		pos++
		if c == '.' {
			continue
		}
		if c == '\n' {
			break
		}
		if c == '\r' {
			continue
		}
		val = val*10 + int64(c-'0')
	}

	if neg {
		return -val, pos
	}
	return val, pos
}

func processChunk(data []byte) *Table {
	table := newTable()
	pos := 0
	limit := len(data)

	for pos < limit {
		start := pos
		for data[pos] != ';' {
			pos++
		}
		stationName := data[start:pos]
		pos++

		temp, nextPos := parseTempFast(data, pos)
		pos = nextPos

		s := table.getOrCreate(stationName)
		if temp < s.min {
			s.min = temp
		}
		if temp > s.max {
			s.max = temp
		}
		s.sum += temp
		s.count++
	}
	return table
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
		return nil, nil, err
	}

	addr, err := windows.MapViewOfFile(handle, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		windows.CloseHandle(handle)
		f.Close()
		return nil, nil, err
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

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <measurements_file>")
		return
	}

	data, cleanup, err := mmapFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer cleanup()

	startTime := time.Now()
	numWorkers := runtime.GOMAXPROCS(0)
	chunkSize := len(data) / numWorkers

	var wg sync.WaitGroup
	results := make([]*Table, numWorkers)

	start := 0
	for i := 0; i < numWorkers; i++ {
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(data)
		} else {
			end = findChunkBoundary(data, end)
		}

		wg.Add(1)
		go func(idx int, chunk []byte) {
			defer wg.Done()
			results[idx] = processChunk(chunk)
		}(i, data[start:end])
		start = end
	}
	wg.Wait()

	finalMap := make(map[string]*Stats)
	for _, t := range results {
		for i := 0; i < tableSize; i++ {
			if t.used[i] {
				nameStr := string(t.entries[i].name)
				if existing, ok := finalMap[nameStr]; ok {
					if t.entries[i].min < existing.min {
						existing.min = t.entries[i].min
					}
					if t.entries[i].max > existing.max {
						existing.max = t.entries[i].max
					}
					existing.sum += t.entries[i].sum
					existing.count += t.entries[i].count
				} else {
					s := t.entries[i]
					finalMap[nameStr] = &s
				}
			}
		}
	}

	stations := make([]string, 0, len(finalMap))
	for s := range finalMap {
		stations = append(stations, s)
	}
	sort.Strings(stations)

	fmt.Print("{")
	for i, name := range stations {
		s := finalMap[name]
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f", name, float64(s.min)/10.0, (float64(s.sum)/float64(s.count))/10.0, float64(s.max)/10.0)
	}
	fmt.Println("}")

	fmt.Printf("\nDone in: %v\n", time.Since(startTime))
}
