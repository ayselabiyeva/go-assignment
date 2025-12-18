# gosort — Concurrent Chunk Sorting (Go)

Random range (`-r`): integers in **[-1000, 1000]** (inclusive).

## Build / Run

### Mode 1: Random numbers
```bash
go run . -r 50
# or
go build -o gosort && ./gosort -r 50
```

### Mode 2: Input file
```bash
go run . -i input.txt
```
`input.txt` format: one integer per line. Empty lines are ignored. Any invalid line causes an error.

### Mode 3: Directory mode
```bash
go run . -d incoming
```
Processes only `.txt` files in `incoming/` and writes sorted versions into a sibling directory:
`incoming_sorted_leisa_eva_231ADB279`

## Design notes

- Chunk count: `max(4, ceil(sqrt(n)))`.
- Chunk sizes are roughly equal: size difference is at most 1 element.
- Each chunk is sorted in its own goroutine using `sort.Ints` and `sync.WaitGroup`.
- Merge is a **k-way merge** using a custom min-heap (no flatten + re-sort).
- Directory mode prints only completion + output directory (no per-file chunk prints), as required.
