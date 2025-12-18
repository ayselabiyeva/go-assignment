package main

/*
Name: Aysel Abiyeva
Student ID: 231ADB279
Program: gosort - Concurrent Chunk Sorting
*/

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"strconv"

)

func main() {
	rN := flag.Int("r", -1, "generate N random integers (N >= 10)")
	inFile := flag.String("i", "", "read integers from input file (one per line)")
	inDir := flag.String("d", "", "process a directory containing .txt files (each sorted independently)")
	flag.Parse()

	modes := 0
	if *rN != -1 {
		modes++
	}
	if strings.TrimSpace(*inFile) != "" {
		modes++
	}
	if strings.TrimSpace(*inDir) != "" {
		modes++
	}
	if modes != 1 {
		usageAndExit("Please specify exactly one mode: -r N OR -i input.txt OR -d incoming")
	}

	if *rN != -1 {
		if err := runRandom(*rN); err != nil {
			log.Fatal(err)
		}
		return
	}

	if strings.TrimSpace(*inFile) != "" {
		if err := runInputFile(*inFile); err != nil {
			log.Fatal(err)
		}
		return
	}

	if strings.TrimSpace(*inDir) != "" {
		if err := runDirectory(*inDir); err != nil {
			log.Fatal(err)
		}
		return
	}
}

func usageAndExit(msg string) {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  gosort -r N")
	fmt.Fprintln(os.Stderr, "  gosort -i input.txt")
	fmt.Fprintln(os.Stderr, "  gosort -d incoming")
	os.Exit(1)
}

// -r random

func runRandom(n int) error {
	if n < 10 {
		return errors.New("N must be >= 10")
	}

	numbers := generateRandomNumbers(n)

	fmt.Println("Original numbers (unsorted):")
	fmt.Println(numbers)

	chunks := splitIntoChunks(numbers)

	fmt.Println("\nChunks before sorting:")
	printChunks(chunks)

	sortedChunks := sortChunksConcurrently(chunks)

	fmt.Println("\nChunks after sorting:")
	printChunks(sortedChunks)

	result := mergeSortedChunks(sortedChunks)

	fmt.Println("\nFinal merged sorted result:")
	fmt.Println(result)

	return nil
}

// -i input file

func runInputFile(path string) error {
	nums, err := readIntsFromFile(path)
	if err != nil {
		return err
	}
	if len(nums) < 10 {
		return fmt.Errorf("input must contain at least 10 valid integers; got %d", len(nums))
	}

	fmt.Println("Original numbers (unsorted):")
	fmt.Println(nums)

	chunks := splitIntoChunks(nums)

	fmt.Println("\nChunks before sorting:")
	printChunks(chunks)

	sortedChunks := sortChunksConcurrently(chunks)

	fmt.Println("\nChunks after sorting:")
	printChunks(sortedChunks)

	result := mergeSortedChunks(sortedChunks)

	fmt.Println("\nFinal merged sorted result:")
	fmt.Println(result)

	return nil
}

// -d directory

func runDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("directory not found: %s", dir)
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", dir)
	}

	parent := filepath.Dir(filepath.Clean(dir))

	firstname := "Aysel"
	surname := "Abiyeva"
	studentID := "231ADB279"
	outDirName := fmt.Sprintf("%s_sorted_%s_%s_%s", filepath.Base(filepath.Clean(dir)), firstname, surname, studentID)
	outDir := filepath.Join(parent, outDirName)

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", outDir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.ToLower(filepath.Ext(e.Name())) != ".txt" {
			continue // ignore non-txt
		}

		inPath := filepath.Join(dir, e.Name())
		nums, err := readIntsFromFile(inPath)
		if err != nil {
			return fmt.Errorf("file %s: %w", e.Name(), err)
		}
		if len(nums) < 10 {
			return fmt.Errorf("file %s: fewer than 10 valid integers (%d)", e.Name(), len(nums))
		}

		chunks := splitIntoChunks(nums)
		sortedChunks := sortChunksConcurrently(chunks)
		result := mergeSortedChunks(sortedChunks)

		outPath := filepath.Join(outDir, e.Name())
		if err := writeIntsToFile(outPath, result); err != nil {
			return fmt.Errorf("failed writing %s: %w", outPath, err)
		}
	}

	fmt.Println("Directory mode complete.")
	fmt.Println("Output directory:", outDir)

	return nil
}

// chunking

func splitIntoChunks(numbers []int) [][]int {
	n := len(numbers)

	numChunks := int(math.Ceil(math.Sqrt(float64(n))))
	if numChunks < 4 {
		numChunks = 4
	}
	if numChunks > n {
		numChunks = n
	}

	base := n / numChunks
	rem := n % numChunks 

	chunks := make([][]int, 0, numChunks)
	start := 0
	for i := 0; i < numChunks; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		chunk := make([]int, size)
		copy(chunk, numbers[start:end])
		chunks = append(chunks, chunk)
		start = end
	}
	return chunks
}

// concurrent sorting

func sortChunksConcurrently(chunks [][]int) [][]int {
	var wg sync.WaitGroup
	wg.Add(len(chunks))

	for i := range chunks {
		i := i
		go func() {
			defer wg.Done()
			sort.Ints(chunks[i])
		}()
	}

	wg.Wait()
	return chunks
}

// merge logic

type heapItem struct {
	val      int
	chunkIdx int
	posIdx   int
}

type minHeap struct {
	data []heapItem
}

func (h *minHeap) Len() int { return len(h.data) }

func (h *minHeap) push(x heapItem) {
	h.data = append(h.data, x)
	h.siftUp(len(h.data) - 1)
}

func (h *minHeap) pop() (heapItem, bool) {
	if len(h.data) == 0 {
		return heapItem{}, false
	}
	top := h.data[0]
	last := h.data[len(h.data)-1]
	h.data = h.data[:len(h.data)-1]
	if len(h.data) > 0 {
		h.data[0] = last
		h.siftDown(0)
	}
	return top, true
}

func (h *minHeap) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if h.data[p].val <= h.data[i].val {
			break
		}
		h.data[p], h.data[i] = h.data[i], h.data[p]
		i = p
	}
}

func (h *minHeap) siftDown(i int) {
	n := len(h.data)
	for {
		l := 2*i + 1
		r := 2*i + 2
		smallest := i

		if l < n && h.data[l].val < h.data[smallest].val {
			smallest = l
		}
		if r < n && h.data[r].val < h.data[smallest].val {
			smallest = r
		}
		if smallest == i {
			return
		}
		h.data[i], h.data[smallest] = h.data[smallest], h.data[i]
		i = smallest
	}
}

func mergeSortedChunks(chunks [][]int) []int {
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	out := make([]int, 0, total)

	h := &minHeap{data: make([]heapItem, 0, len(chunks))}
	for ci, c := range chunks {
		if len(c) == 0 {
			continue
		}
		h.push(heapItem{val: c[0], chunkIdx: ci, posIdx: 0})
	}

	for h.Len() > 0 {
		item, _ := h.pop()
		out = append(out, item.val)
		nextPos := item.posIdx + 1
		if nextPos < len(chunks[item.chunkIdx]) {
			h.push(heapItem{
				val:      chunks[item.chunkIdx][nextPos],
				chunkIdx: item.chunkIdx,
				posIdx:   nextPos,
			})
		}
	}

	return out
}

//helpers

func generateRandomNumbers(n int) []int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	nums := make([]int, n)
	for i := 0; i < n; i++ {
		// [-1000, 1000]
		nums[i] = rng.Intn(2001) - 1000
	}
	return nums
}

func printChunks(chunks [][]int) {
	for i, c := range chunks {
		fmt.Printf("Chunk %d: %v\n", i, c)
	}
}

func readIntsFromFile(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		return nil, fmt.Errorf("cannot open file %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)

	// In case lines are very long (not expected here, but safe)
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, maxCapacity)

	nums := make([]int, 0, 64)
	lineNo := 0

	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())

		// Remove UTF-8 BOM if it exists on the first line (common on Windows)
		if lineNo == 1 {
			line = strings.TrimPrefix(line, "\uFEFF")
			line = strings.TrimSpace(line)
		}

		if line == "" {
			continue // ignore empty lines
		}

		val, convErr := parseIntStrict(line)
		if convErr != nil {
			return nil, fmt.Errorf("file %s: invalid integer on line %d: %q", path, lineNo, line)
		}
		nums = append(nums, val)
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("file %s: read error: %w", path, err)
	}

	return nums, nil
}


func parseIntStrict(s string) (int, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "\uFEFF") // handle BOM (sometimes in first line)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}

	// Only allow optional leading +/- and then digits
	start := 0
	if s[0] == '+' || s[0] == '-' {
		start = 1
		if len(s) == 1 {
			return 0, fmt.Errorf("sign without digits")
		}
	}

	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("non-digit character")
		}
	}

	return strconv.Atoi(s)
}


func writeIntsToFile(path string, nums []int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, v := range nums {
		if _, err := fmt.Fprintln(w, v); err != nil {
			return err
		}
	}
	return w.Flush()
}
