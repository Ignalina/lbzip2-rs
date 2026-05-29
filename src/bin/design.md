# lbunzip2 CLI Design

Reference CLI binary for lbzip2-rs. Demonstrates the chunk revolver
pipeline for decompressing large bzip2 files.

## Chunk Revolver Pipeline

Three-stage pipeline with 4-slot ring buffer:

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Reader   │──→│   Decode  │──→│  Writer   │
│  thread   │    │   (main)  │    │  thread   │
└──────────┘     └──────────┘     └──────────┘
     ↑                │
     └── slot pool ───┘   (4 pre-allocated buffers recycled)
```

### Slot Pool (Zero-Copy)

4 pre-allocated buffers of 232 MB each (200 MB data + 32 MB headroom).
Buffers circulate between reader and decoder — never reallocated.

```
Slot layout:  [32 MB headroom][────── 200 MB data ──────]
               ↑ tiny carry     ↑ reader fills here
               copied here      (zero copy — stays in place)
```

The 200 MB raw read data is **never copied**. Only the carry (unconsumed
tail from previous chunk, typically < 13 MB) is copied into the headroom
area so `decode_chunk` sees one contiguous `&[u8]`.

### Reader Thread

Receives empty slots from the pool. Reads 200 MB of compressed data
into `slot[HEADROOM..]`. Sends `(slot, read_len, is_last)` to decode.

### Main Thread (Decode)

1. Receives filled slot
2. Copies tiny carry into headroom (< 13 MB)
3. Calls `decode_chunk_segments()` — parallel decode via scoped-thread `par_map`
4. Saves new carry (unconsumed tail)
5. Recycles slot back to reader pool
6. Sends each decoded segment to writer individually (no assembly memcpy)

### Writer Thread

Receives decoded segments via `sync_channel(4)`. Writes to disk with
`BufWriter`. The 4-slot channel means decode never blocks on slow writes.

### Why 4 Slots

- Reader can pre-fill 2–3 slots while decode is busy
- Writer can drain 2–3 slots while decode produces more
- 4 × 232 MB ≈ 1 GB memory for the slot pool

## Future Ideas

### Contiguous Ring Buffer

Allocate all 4 slots in one contiguous region:

```
[headroom][── slot 0 ──][── slot 1 ──][── slot 2 ──][── slot 3 ──]
```

- Slots 0→1, 1→2, 2→3: carry is already adjacent to next slot — zero copy.
- Slot 3→0 (wrap): copy carry to headroom (1 in 4 chunks only).

Requires `unsafe` for shared buffer between reader and main thread.
Practical gain is small: 13 MB copy ≈ 1 ms vs ~2 s decode = 0.05%.

### mmap Instead of Reader Thread

For local NVMe, mmap could replace the reader thread entirely:

```rust
let mmap = unsafe { Mmap::map(&file) };
// Just slide a pointer — OS handles prefetch
let data = &mmap[offset..offset + CHUNK_SIZE];
```

No reader thread, no ring buffer, no carry at all.
The kernel sequential prefetcher handles read-ahead.
Less suitable for slow/remote filesystems.

## Performance

Target: 147 GB planet-241021.osm.bz2, 8-core / 16-thread Thinkpad T14s.

| Metric                 | Value              |
|------------------------|--------------------|
| Chunk size             | 200 MB             |
| Ring slots             | 4                  |
| Carry headroom         | 32 MB              |
| CPU usage              | ~1300% (13 cores)  |
| Compressed throughput  | ~22 MB/s           |
| Decompressed throughput| ~233 MB/s          |
| vs C lbzip2            | 4% slower          |
