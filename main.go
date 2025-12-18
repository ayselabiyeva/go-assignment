package main

/*
Name: Leisa Eva
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
)

// NOTE: Random range for -r mode:
// Generates integers in the inclusive range [-1000, 1000].

func main() {
	// Exactly one mode must be selected.
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

// -----------------------------
// Mode 1: -r (random)
// -----------------------------

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

// -----------------------------
// Mode 2: -i (input file)
// -----------------------------

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

// -----------------------------
// Mode 3: -d (directory)
// -----------------------------

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

	// Output directory: sibling of "dir"
	parent := filepath.Dir(filepath.Clean(dir))

	// Exact naming pattern required:
	// incoming_sorted_<firstname>_<surname>_<studentID>
	firstname := "leisa"
	surname := "eva"
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

	// No per-file console output per spec; still helpful to print final directory once.
	fmt.Println("Directory mode complete.")
	fmt.Println("Output directory:", outDir)

	return nil
}

// -----------------------------
// Chunking logic
// -----------------------------

func splitIntoChunks(numbers []int) [][]int {
	n := len(numbers)

	numChunks := int(math.Ceil(math.Sqrt(float64(n))))
	if numChunks < 4 {
		numChunks = 4
	}
	if numChunks > n {
		// In practice, assignment ensures n >= 10, so this only matters for tiny n.
		numChunks = n
	}

	// Roughly equal sizes: difference at most 1.
	base := n / numChunks
	rem := n % numChunks // first rem chunks get one extra element

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

// -----------------------------
// Concurrent sorting
// -----------------------------

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

// -----------------------------
// Merge logic (k-way merge, no re-sort)
// -----------------------------

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

// -----------------------------
// Helpers
// -----------------------------

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
		return nil, err
	}
	defer f.Close()

	var nums []int
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		val, convErr := parseIntStrict(line)
		if convErr != nil {
			return nil, fmt.Errorf("invalid integer on line %d: %q", lineNo, line)
		}
		nums = append(nums, val)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nums, nil
}

func parseIntStrict(s string) (int, error) {
	// Reject trailing junk (e.g. "12abc") by scanning an extra token.
	var v int
	var extra string
	n, err := fmt.Sscan(s, &v, &extra)
	if err != nil {
		return 0, err
	}
	if n != 1 {
		return 0, errors.New("not a pure integer")
	}
	return v, nil
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
