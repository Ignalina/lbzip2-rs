# lbzip2-rs Design

Pure Rust parallel bzip2 decompressor — library + CLI.

## CLI Pipeline (lbunzip2)

Worker pool pipeline with 6-slot ring buffer:

```
Reader ──→ Main (carry+split) ──→ 32 Workers ──→ Collector ──→ Writer
  ↑                                                    │
  └──────────── slot recycle ──────────────────────────┘
```

**5 threads + 32 worker threads:**
- **Reader** — fills slots with 200 MB compressed data
- **Main** — copies carry into headroom, splits boundaries, posts work items, loops immediately
- **32 Workers** — persistent threads, each pulls work items, decodes segments, sends results
- **Collector** — gathers results per slot, sends to writer when slot complete, recycles slot
- **Writer** — writes to disk via BufWriter

**Ring buffer**: 6 pre-allocated slots, each `[32 MB headroom | 200 MB chunk]`.
Slots are never freed — recycled through `slot_return` channel.

**Carry**: between chunks, the unconsumed tail (< 1 MB) is copied into the
headroom area of the next slot. The 200 MB read data stays in place — never copied.

**No barriers**: main thread posts work and loops immediately. Workers that finish
their segment move to the next slot's work — no idle time between chunks.

**Per-chunk cycle** (main thread perspective):
1. Receive filled slot from reader (~0ms if slots available)
2. Copy carry into headroom (~1.8ms)
3. Split into 32 segment boundaries (~3-14ms via rayon)
4. Compute new carry from split boundaries
5. Post 31 work items to worker queue
6. **Loop immediately** — no wait for decode

### Constants
```rust
CHUNK_SIZE     = 200 MB    // compressed data per slot
BUF_CAP        = 4 MB      // BufReader/BufWriter capacity
RING_SLOTS     = 6         // number of pre-allocated slot buffers
CARRY_HEADROOM = 32 MB     // space for carry at start of each slot
SLOT_SIZE      = 232 MB    // CARRY_HEADROOM + CHUNK_SIZE
```

## Parallel Split + Decode

### O(N) Boundary Finding

Not a full scan. N evenly-spaced positions, each forward-scans ~500 bytes
for the next BLOCK_MAGIC, then 73-bit quick-verify. **Total: 3-14ms for 200 MB.**
Uses rayon thread pool for the split only.

### Segment Decode

Chunk split into 32 segments (= n_workers, one per core).
Each segment decoded by one persistent worker thread.
Within a segment: sequential bitstream walk (Huffman → MTF → BWT → RLE2).
Output is a heap-allocated Vec<u8> per segment (~60 MB typical).

### pbzip2 Concatenated Streams

Planet files are ~1.3M mini-streams concatenated (~120 KB each).
FINAL_MAGIC appears every ~120 KB. Each segment decoder handles:
FINAL_MAGIC → skip CRC + pad + BZhN header → continue to next BLOCK_MAGIC

## Block Decode

```
BitReader (64-bit buffer, bulk 8-byte refill)
  → Header (CRC + orig_ptr + bitmap + selectors)
  → Huffman (10-bit packed u16 lookup, 2KB/table, L1-resident)
  → MTF (fast-path n=0, n=1)
  → RLE1 (RUNA/RUNB) → tt[] array (~3.6 MB)
  → Inverse BWT (in-place T-transformation)
  → RLE2 (pointer chase with 2-step prefetch, raw pointer output)
  → Vec<u8>
```

**Refactored internals:**
- `decode_block_common()` — shared header/huffman/MTF/BWT logic
- `rle2_decode_alloc()` — RLE2 into new Vec<u8> (used by `decode_block()`)
- `rle2_decode_into()` — RLE2 into caller-provided `&mut [u8]` (used by `decode_block_into()`)

**Thread-local tt pool**: 3.6 MB buffer reused per thread, no alloc/free per block.
**Zero heap allocs per block**: Huffman tables, selectors, bitmaps all on stack.

## Public API (chunk.rs)

- `ChunkDecoder::from_header(header)` — parse bzip2 header
- `ChunkDecoder::decode_chunk(data, is_last)` — full parallel decode, returns concatenated output
- `ChunkDecoder::decode_chunk_segments(data, is_last)` — parallel decode, returns segments separately
- `split_chunk(data, n_segments, max_blocksize, is_last)` — split into boundaries (used by worker pool)
- `decode_segment(data, start_bit, end_bit, max_blocksize)` — decode single segment (used by workers)

## bzip2 Stream Format

```
[BZh9]  [BLOCK_MAGIC block_data]...  [FINAL_MAGIC crc32] [pad]
```

- Block magic: 48-bit `0x314159265359` (π digits), **bit-aligned** (not byte-aligned)
- End-of-stream: 48-bit `0x177245385090` (√π digits) + CRC32 + byte-pad

### False Positive Rejection

73-bit quick-verify after magic: randomised=0, orig_ptr < max_blocksize, bitmap ≠ 0.

## Module Map

```
src/
├── lib.rs           # BLOCK_MAGIC/FINAL_MAGIC, dedicated rayon pool (LBZIP2_THREADS)
├── bitreader.rs     # 64-bit buffered reader, arbitrary bit offset, peek/consume
├── block.rs         # Single block: decode_block_common + rle2_decode_alloc/into
├── block_scan.rs    # 48-bit scanner, split_boundaries_parallel, quick_verify
├── bwt.rs           # Inverse Burrows-Wheeler (in-place T-transformation)
├── chunk.rs         # split_chunk, decode_segment, ChunkDecoder, pbzip2 support
├── huffman.rs       # 10-bit packed lookup + tree fallback
├── mtf.rs           # Move-to-front: fast n=0/n=1
├── parallel.rs      # In-memory parallel (small files)
├── reader.rs        # StreamingBz2Read + ParallelBz2Read (mmap)
├── stream.rs        # Sequential decoder (reference path)
└── bin/lbunzip2.rs  # CLI: worker pool pipeline (reader + main + 32 workers + collector + writer)
```

## Benchmarks

### Odin — Threadripper PRO 3975WX, 32 cores, 512 GB DDR4

**Liechtenstein** (5.2 MB → 60 MB, 71 blocks):

| Mode | Time | Throughput | vs C |
|---|---|---|---|
| C libbz2 (single-thread) | 870 ms | 69 MB/s | 1.0× |
| lbzip2-rs (single-thread) | 564 ms | 107 MB/s | 1.5× |
| lbzip2-rs (parallel, 32 threads) | 89 ms | 676 MB/s | 9.8× |

**Stage breakdown** (single-thread):

| Stage | Time | % |
|---|---|---|
| Header + bitmap + selectors + trees | 4 ms | 0.7% |
| Huffman decode + MTF + RLE1 | 129 ms | 22.7% |
| Inverse BWT | 121 ms | 21.3% |
| **RLE2 + output** | **314 ms** | **55.2%** |
| **Total** | **568 ms** | |

RLE2 dominates: dependent pointer chain through ~3.6 MB random-access tt[].
Memory-latency-bound. 2-step prefetch helps but cannot break the serial dependency.

**Planet 1 GB slice** (32 cores):

| Architecture | Time | Throughput |
|---|---|---|
| Old (rayon barrier, 4 slots, 255 segments) | 5.5s | 1804 MB/s |
| **New (worker pool, 6 slots, 32 segments)** | **5.2s** | **1909 MB/s** |

**Per-slot timing (worker pool):**

| Phase | Time | Notes |
|---|---|---|
| Reader I/O | 50-90ms | NVMe |
| Carry copy | ~1.8ms | Tiny |
| Split boundaries | 3-14ms | Via rayon |
| Main thread total | ~5-16ms | Posts work, loops immediately |
| Workers decode (wall) | ~1000ms | 32 segments across all workers |

**Barrier waste eliminated:** Old architecture wasted ~700ms/slot in rayon
collect() barrier. Over planet 147 GB (~735 slots) = ~515s of idle CPU time removed.

### Timing Feature
`cargo build --release --features timing` — instrumented stderr output.
Code in `chunk.rs` and `bin/lbunzip2.rs`, `#[cfg(feature = "timing")]`.
Segment CSV: `/tmp/lbzip2_segments.csv`.

## Performance Philosophy

The bzip2 algorithm is from 1996 when computation was the bottleneck and memory
was fast. Modern hardware inverted this. The algorithm is identical — the
mechanics are adapted:

| What | Why |
|---|---|
| Thread-local tt buffers | Avoid alloc/free contention across 32 cores |
| 2-step BWT prefetch | Hide 100+ cycle L3 latency |
| Packed u16 Huffman tables | All 6 tables (12 KB) fit L1 |
| 64-bit bulk bitreader | One load vs byte-at-a-time |
| Raw pointer RLE2 output | No bounds check per byte, memset for repeats |
| Group-of-50 inner loop | Fixed tree pointer, no per-symbol branch |
| Worker pool (no barrier) | Cores never idle between chunks |
| 6-slot ring buffer | Reader stays ahead, workers always have work |

---

## History

### v0.3.0 "Sleipner" — rayon barrier architecture

3-thread pipeline (reader + main + writer), 4 ring slots.
rayon `par_iter().collect()` with 255 segments (8× oversplit).
5.5s / 1804 MB/s on 1 GB planet slice (32 cores).

Barrier waste ~700ms/slot: all cores idle between chunks during carry + send.

### Earlier Benchmark Numbers (Loki — Ryzen 9 7900, 12 cores)

| Mode | Throughput | vs C libbz2 |
|---|---|---|
| C libbz2 (single-thread) | 108 MB/s | 1.0× |
| lbzip2-rs (single-thread) | 143 MB/s | 1.3× |
| lbzip2-rs (parallel, 12 threads) | 731 MB/s | 6.8× |
