# design_for_claude.md — Read This First Every Session

**Purpose**: Stop the rediscovery loop. Read this file + `design.md` before touching any code.
`design.md` is the human-readable design doc (Rickard maintains it).
This file adds AI-specific context: what NOT to do, hardware, timing data, session protocol.

**Session start protocol**:
1. Read `design_for_claude.md` (this file)
2. Read `design.md` (architecture, module map, benchmarks)
3. Only then ask what to work on

---

## 1. Core Design Principles

1. **ZERO ALLOCATIONS in the hot path.** Pre-allocate everything. Reuse buffers.
   Only large one-time allocations are acceptable (ring slots, output buffer).
2. **Zero-copy.** The 200 MB compressed data stays in place. Send offsets, not data.
3. **Simple parallelism.** N cores = N workers pulling from a work list. No magic.
4. **The iBWT+RLE2 optimization is sacred.** 1.5x faster than C libbz2 single-thread.
   2-step prefetch, raw pointer output, memset repeats. DO NOT TOUCH.

---

## 2. What This Crate Does

Pure Rust parallel bzip2 decompressor. No C/FFI. Two interfaces:
- **Library** (`lbzip2::chunk::ChunkDecoder`) -- zero-copy, in-process
- **CLI** (`lbunzip2`) -- worker pool pipeline for file-to-file decompression

Primary use case: decompressing 147 GB planet.osm.bz2 (~1.5 TB decompressed)
as part of the [osm-katana](https://github.com/Ignalina/katana-osm) pipeline.

---

## 3. CLI Pipeline Architecture (lbunzip2)

**Worker pool pipeline -- NO rayon barrier:**

```
Reader --> Main (carry+split) --> 32 Workers --> Collector --> Writer
  ^                                                    |
  +------------- slot recycle -------------------------+
```

### Key design points
- **Main thread does NOT decode.** It does carry + split + post work items, then
  loops immediately to the next slot. Total main thread time: ~5-16ms per slot.
- **Workers are persistent** std::thread (not rayon). They pull from a shared work
  channel via Arc<Mutex<Receiver>>. When done with slot N's segment, they immediately
  grab slot N+1's work.
- **Collector thread** gathers results, writes completed slots to writer in order,
  recycles slot buffers back to reader.
- **No barrier.** Workers never idle between slots.

### Ring Buffer ("Slot Pool")
- **6 pre-allocated slots** (RING_SLOTS = 6), each 232 MB
- Slot layout: [32 MB headroom][200 MB chunk data]
- Slots are never freed -- recycled via slot_return channel
- 6 slots allows: ~1 reading, ~3 being decoded, ~1 being written, ~1 recycling

### Carry Mechanism (zero-copy for the 200 MB)
- Between chunks, a small "carry" of unconsumed bytes (typically < 1 MB)
- Carry is copied into the headroom area of the next slot, just before the read data
- This makes carry + new_data contiguous without copying the 200 MB chunk

### Data Flow Per Slot
1. **Reader** fills slot with 200 MB compressed data (~50-90ms on NVMe)
2. **Main thread** receives slot, copies tiny carry into headroom (~1.8ms)
3. **Main thread** calls split_chunk() -> 32 segment boundaries (~3-14ms)
4. **Main thread** computes new carry from split boundaries (immediate, no decode wait)
5. **Main thread** posts 31 work items to worker channel, loops to next slot
6. **Workers** decode segments -> send (chunk_id, seg_id, Vec<u8>) to collector
7. **Collector** accumulates results per slot, when all 32 done -> sends to writer
8. **Writer** writes segment outputs to disk, collector recycles slot

### Constants
```rust
CHUNK_SIZE     = 200 MB    // compressed data per slot
BUF_CAP        = 4 MB      // BufReader/BufWriter capacity
RING_SLOTS     = 6         // number of pre-allocated slot buffers
CARRY_HEADROOM = 32 MB     // space for carry at start of each slot
SLOT_SIZE      = 232 MB    // CARRY_HEADROOM + CHUNK_SIZE
```

---

## 4. Parallel Split + Decode Strategy

### Parallel Offset Finding (via rayon, O(N))
This is the key insight -- NOT a full scan:
1. Calculate n_splits = n_workers evenly-spaced nominal positions in 200 MB
2. Forward-scan from each nominal position for the next BLOCK_MAGIC
3. Quick-verify with 73-bit header check (rejects false positives instantly)
4. Each scan covers only ~500 bytes forward
5. **Cost: 3-14ms** (negligible -- DO NOT try to optimize this)

Split still uses the rayon thread pool (lib.rs). Workers use std::thread.

### Segment Decode
- Chunk split into 32 segments (= n_workers, one per physical core)
- Each segment decoded by one persistent worker thread
- Within a segment: Huffman -> MTF -> BWT -> RLE2 (no further parallelism needed)
- Output: heap-allocated Vec<u8> per segment (~60 MB typical)

### pbzip2 Concatenated Streams
Planet files are ~1.3M mini-streams concatenated (~120 KB each).
FINAL_MAGIC appears every ~120 KB. Each segment decoder handles:
FINAL_MAGIC -> skip CRC + pad + BZhN header -> continue to next BLOCK_MAGIC

---

## 5. Allocation Policy

### What's good (keep):
- Ring slots: 6 x 232 MB, pre-allocated once, recycled forever
- tt[] buffer: thread-local, reused across blocks (3.6 MB per thread)
- Huffman tables, selectors, bitmaps: all stack-allocated, zero heap

### Current output allocation:
- Each segment produces one Vec<u8> (~60 MB) allocated on the heap
- Worker sends Vec<u8> through channel to collector
- Heap allocation cost is ~microseconds for 60 MB (negligible vs ~100ms decode)
- Previously considered pre-allocated output buffers but heap is simpler and same speed

---

## 6. Block Decode Pipeline (per block, per thread) -- DO NOT CHANGE

```
BitReader (64-bit buffer, bulk 8-byte refill)
  -> Header (CRC + orig_ptr + bitmap + selectors)
  -> Huffman (10-bit packed u16 lookup, 2KB/table, L1-resident)
  -> MTF (fast-path n=0, n=1)
  -> RLE1 (RUNA/RUNB) -> tt[] array (~3.6 MB, thread-local pool)
  -> Inverse BWT (in-place T-transformation)
  -> RLE2 (2-step prefetch pointer chase, raw pointer output, memset repeats)
  -> output bytes
```

**This pipeline is 1.5x faster than C libbz2 single-threaded.**
The iBWT+RLE2 prefetch optimization is the core of that speedup.
All Huffman tables, selectors, bitmaps are stack-allocated -- zero heap.

### block.rs refactoring:
- `decode_block_common()` -- shared header/huffman/MTF/BWT logic, returns (tt, t_pos)
- `rle2_decode_alloc()` -- RLE2 into new Vec<u8> (used by decode_block())
- `rle2_decode_into()` -- RLE2 into caller &mut [u8] (used by decode_block_into())
- `decode_block_into()` exists for future pre-allocated buffer work (not currently used)

---

## 7. Stage Breakdown (Single-Thread, Odin, Liechtenstein)

| Stage | Time | % |
|---|---|---|
| Header + bitmap + selectors + trees | 4 ms | 0.7% |
| Huffman decode + MTF + RLE1 | 129 ms | 22.7% |
| Inverse BWT | 121 ms | 21.3% |
| **RLE2 + output** | **314 ms** | **55.2%** |
| **Total** | **568 ms** | |

RLE2 dominates: dependent pointer chain through ~3.6 MB random-access tt[].
Memory-latency-bound. Prefetch helps but cannot break serial dependency.

---

## 8. Timing Data (Planet 1GB, Odin, 32 cores)

### Worker pool pipeline (current):

| Phase | Time | Notes |
|---|---|---|
| Reader I/O | 50-90ms | NVMe |
| Carry copy | ~1.8ms | Tiny |
| Split boundaries | 3-14ms | Via rayon thread pool |
| Main thread total | ~5-16ms | Posts work, loops immediately |
| Workers decode (wall) | ~1000ms | 32 segments across all workers |

**Total: 5.2s, 1909 MB/s** (1 GB -> 9.86 GB decompressed)

### Comparison with old architecture:

| | Old (rayon barrier) | New (worker pool) |
|---|---|---|
| Architecture | par_iter().collect() | 32 persistent workers |
| Slots | 4 | 6 |
| Segments per chunk | 255 | 32 |
| Barrier waste | ~700ms/slot | ~0ms |
| 1 GB time | 5.5s | 5.2s |
| Throughput | 1804 MB/s | 1909 MB/s |

### Timing Feature
`cargo build --release --features timing` -- instrumented stderr output.
Code in `chunk.rs` and `bin/lbunzip2.rs`, `#[cfg(feature = "timing")]`.
Segment CSV: `/tmp/lbzip2_segments.csv`.

---

## 9. Module Map

```
src/
  lib.rs           # BLOCK_MAGIC/FINAL_MAGIC, dedicated rayon pool (LBZIP2_THREADS)
  bitreader.rs     # 64-bit buffered reader, arbitrary bit offset, peek/consume
  block.rs         # decode_block_common + rle2_decode_alloc/into, thread-local tt pool
  block_scan.rs    # 48-bit scanner, split_boundaries_parallel, quick_verify
  bwt.rs           # Inverse BWT (in-place T-transformation)
  chunk.rs         # split_chunk, decode_segment, ChunkDecoder, pbzip2 support
  huffman.rs       # 10-bit packed lookup + tree fallback
  mtf.rs           # Move-to-front: fast n=0/n=1
  parallel.rs      # In-memory parallel (small files only)
  reader.rs        # StreamingBz2Read + ParallelBz2Read
  stream.rs        # Sequential decoder (reference/benchmark)
  bin/lbunzip2.rs  # CLI: worker pool pipeline (reader + main + 32 workers + collector + writer)
```

### Public API (chunk.rs):
- `ChunkDecoder::from_header(header)` -- parse bzip2 header
- `ChunkDecoder::decode_chunk(data, is_last)` -- full parallel decode (still uses rayon internally)
- `ChunkDecoder::decode_chunk_segments(data, is_last)` -- parallel, returns segments separately
- `split_chunk(data, n_segments, max_blocksize, is_last)` -- split boundaries (for worker pool)
- `decode_segment(data, start_bit, end_bit, max_blocksize)` -- decode single segment (for workers)

---

## 10. Build & Test

```bash
cargo build --release                    # normal build
cargo build --release --features timing  # with timing instrumentation
cargo test                               # 25 unit tests
cargo test --release --test decompress_bench -- --nocapture --ignored  # benchmark
cargo test --release --test stage_breakdown -- --nocapture --ignored   # hotspot analysis
LBZIP2_THREADS=8 cargo run --release --bin lbunzip2 -- input.bz2 out  # override threads
```

---

## 11. Test Data

- `test_data/hello.bz2` -- 14 bytes -> "Hello, World!\n"
- `test_data/liechtenstein.osm.bz2` -- 5.2 MB -> 60 MB (71 blocks)
- `/home/rickard/work/planet-241021.osm.bz2` -- 147 GB (~1.3M mini-streams)
- `/home/rickard/work/planet_1g.bz2` -- 1 GB snippet for quick tests

---

## 12. Hardware

| Machine | CPU | Cores | SMT | RAM | L2 | L3 | Storage |
|---|---|---|---|---|---|---|---|
| **odin** | Threadripper PRO 3975WX (Zen2) | 32 | OFF | 512 GB DDR4 | 16 MB | 128 MB | 2x Samsung 1735 RAID-0 XFS |
| **loki** | Ryzen 9 7900 (Zen4) | 12 | OFF | 64 GB DDR5 | 12 MB | 64 MB | Corsair MP700 Pro PCIe 5.0 |
| **laptop** | T14s 2024, Ryzen AI 360 (Zen5) | 8+SMT | ON | 64 GB DDR5 | 8 MB | 24 MB | Kioxia NVMe |

---

## 13. Common Mistakes -- DO NOT DO THESE

1. **Do NOT re-analyze the codebase from scratch** -- read this doc + design.md first.
2. **Do NOT optimize split_boundaries_parallel** -- 3-14ms, negligible.
3. **Do NOT suggest "pipelining the scan ahead"** -- 3ms vs 1000ms decode.
4. **Do NOT touch the iBWT+RLE2 decode path** -- it's 1.5x faster than C, it stays.
5. **Do NOT add allocations in the hot path** -- the goal is FEWER allocs, not more.
6. **Do NOT confuse parallel.rs with chunk.rs** -- parallel.rs is for small files only.
7. **Do NOT forget pbzip2 streams** -- FINAL_MAGIC every ~120 KB in planet files.
8. **Do NOT claim disk is the bottleneck** -- NVMe reads are 50-90ms, decode is 1000ms.
9. **Do NOT suggest increasing buffer sizes as optimization** -- the ring design is correct.
10. **Do NOT reintroduce rayon barriers** -- the worker pool pipeline eliminated collect() barriers.
11. **Do NOT reduce RING_SLOTS below 6** -- workers need slots to flow into without waiting.

---

## 14. Znippy Ecosystem

Used by [osm-katana](https://github.com/Ignalina/katana-osm): bz2 -> VTD XML -> PBF.
Library API (ChunkDecoder) designed for zero-copy: caller owns buffers, decoder borrows.
End-to-end planet: ~76 minutes estimated with worker pool, 309 MB/s decompressed.
