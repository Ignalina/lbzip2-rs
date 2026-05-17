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
4. **The iBWT+RLE2 optimization is sacred.** 1.5× faster than C libbz2 single-thread.
   2-step prefetch, raw pointer output, memset repeats. DO NOT TOUCH.

---

## 2. What This Crate Does

Pure Rust parallel bzip2 decompressor. No C/FFI. Two interfaces:
- **Library** (`lbzip2::chunk::ChunkDecoder`) — zero-copy, in-process
- **CLI** (`lbunzip2`) — 3-thread pipeline for file-to-file decompression

Primary use case: decompressing 147 GB planet.osm.bz2 (~1.5 TB decompressed)
as part of the [osm-katana](https://github.com/Ignalina/katana-osm) pipeline.

---

## 3. CLI Pipeline Architecture (lbunzip2)

**Three dedicated threads, connected by channels:**

```
┌─────────────┐     sync_channel      ┌──────────────────┐     sync_channel      ┌──────────────┐
│   READER    │ ──(slot,len,is_last)──→│    MAIN THREAD   │ ──(Vec<u8> segs)───→  │    WRITER    │
│   thread    │                        │  carry + decode   │                        │    thread    │
└──────┬──────┘                        └──────────────────┘                        └──────────────┘
       ↑                                       │
       └───── slot_return channel ─────────────┘
              (recycled 232 MB buffers)
```

### Ring Buffer ("Slot Pool") — the simple idea
- **4 pre-allocated slots** (`RING_SLOTS = 4`), each 232 MB
- Slot layout: `[32 MB headroom][200 MB chunk data]`
- Slots are **never freed** — recycled via `slot_return` channel
- Reader thread has NO WAIT as long as there's a free slot — just fills next one
- Single reader thread, sequential NVMe read — saturates disk bandwidth

### Carry Mechanism (zero-copy for the 200 MB)
- Between chunks, there's a small "carry" of unconsumed bytes (typically < 1 MB)
- Carry is copied into the **headroom area** of the next slot, just before the read data
- This makes `carry + new_data` contiguous without copying the 200 MB chunk
- The 200 MB raw read data **stays in place** — never copied

### Data Flow Per Chunk
1. **Reader** fills slot with 200 MB compressed data (~50-90ms on NVMe)
2. **Main thread** receives slot, copies tiny carry into headroom
3. **Main thread** calls `decoder.decode_chunk_segments(data, is_last)`
   - Inside: ALL cores find block offsets in the 200 MB chunk in parallel (~3-5ms)
   - Inside: ALL cores decode their assigned segments (~1000ms)
   - Returns: `Vec<Vec<u8>>` — each segment's decompressed output separately
4. **Main thread** saves new carry, recycles slot back to reader
5. **Main thread** sends each segment `Vec<u8>` to writer via `write_tx`
6. **Writer** receives segments, writes to disk via `BufWriter`

### Constants
```rust
CHUNK_SIZE     = 200 MB    // compressed data per slot
BUF_CAP        = 4 MB      // BufReader/BufWriter capacity
RING_SLOTS     = 4         // number of pre-allocated slot buffers
CARRY_HEADROOM = 32 MB     // space for carry at start of each slot
SLOT_SIZE      = 232 MB    // CARRY_HEADROOM + CHUNK_SIZE
```

---

## 4. Parallel Split + Decode Strategy

### Parallel Offset Finding (ALL cores, O(N))
This is the key insight — NOT a full scan:
1. Calculate `n_splits = n_threads × oversplit` evenly-spaced nominal positions in 200 MB
2. ALL cores forward-scan from their nominal position for the next BLOCK_MAGIC
3. Quick-verify with 73-bit header check (rejects false positives instantly)
4. Each core scans only ~500 bytes forward → total scan ≈ 8 KB for 200 MB
5. **Cost: 3-5ms** (negligible — DO NOT try to optimize this)

### Segment Decode
- Chunk split at boundaries into ~255 segments (oversplit 8× for load balance)
- Each segment decoded by a worker — sequential bitstream walk per segment
- Within a segment: Huffman → MTF → BWT → RLE2 (no further parallelism needed)

### pbzip2 Concatenated Streams
Planet files are ~1.3M mini-streams concatenated (~120 KB each).
FINAL_MAGIC appears every ~120 KB. Each segment decoder handles:
FINAL_MAGIC → skip CRC + pad + BZhN header → continue to next BLOCK_MAGIC

---

## 5. Allocation Policy — CURRENT vs DESIRED

### What's good (keep):
- Ring slots: 4 × 232 MB, pre-allocated once, recycled forever
- tt[] buffer: thread-local, reused across blocks (3.6 MB per thread)
- Huffman tables, selectors, bitmaps: all stack-allocated, zero heap

### What's bad (current — needs fixing):
- **~255 Vec<u8> allocations per chunk**: each segment's decode output is a
  fresh `Vec<u8>` allocated inside `decode_block()`. For 200 MB → 1900 MB
  decompressed, that's 255 mallocs of ~7 MB each, every chunk cycle.
- **Ownership transfer through channel**: each `Vec<u8>` is moved through
  `sync_channel` to the writer. The `send` phase takes ~550ms.

### Desired: pre-allocated output buffer
- Pre-allocate one large output buffer (~2 GB) for decompressed data
- After offset-finding, we know each segment's position in compressed data
- Each worker writes directly into its slice of the output buffer
- Writer receives `(buffer_ref, offset, length)` — no data movement
- Zero allocs in the hot path. Only the initial buffer allocation.

---

## 6. Rayon Investigation — Open Question

### Observed: CPU oscillates 60-100% on odin (32 cores) and loki (12 cores)
Laptop (8 cores + SMT) stays at ~90%.

### Hypothesis: rayon overhead
- Currently using rayon `par_iter` with 256 work items per chunk
- rayon's work-stealing deque has overhead: wake/sleep/steal cycles
- `par_iter().collect()` is a barrier — all cores must finish before ANY
  result is available
- Between chunks: all cores idle during carry + send phase

### Alternative: fixed worker pool
- N persistent threads (1 per core), each loops on `recv() → decode → send`
- Work list: simple channel/queue of segment descriptors
- No work-stealing overhead, no barrier, no rayon dependency for decode
- Workers can start sending results to writer BEFORE all segments finish

### Status: needs profiling to confirm rayon is the cause vs other factors

---

## 7. Block Decode Pipeline (per block, per thread) — DO NOT CHANGE

```
BitReader (64-bit buffer, bulk 8-byte refill)
  → Header (CRC + orig_ptr + bitmap + selectors)
  → Huffman (10-bit packed u16 lookup, 2KB/table, L1-resident)
  → MTF (fast-path n=0, n=1)
  → RLE1 (RUNA/RUNB) → tt[] array (~3.6 MB, thread-local pool)
  → Inverse BWT (in-place T-transformation)
  → RLE2 (2-step prefetch pointer chase, raw pointer output, memset repeats)
  → output bytes
```

**This pipeline is 1.5× faster than C libbz2 single-threaded.**
The iBWT+RLE2 prefetch optimization is the core of that speedup.
All Huffman tables, selectors, bitmaps are stack-allocated — zero heap.

---

## 8. Stage Breakdown (Single-Thread, Odin, Liechtenstein)

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

## 9. Timing Data (Planet, Odin, 32 cores)

Per-chunk steady state (200 MB compressed → ~1900 MB decompressed):

| Phase | Time | Notes |
|---|---|---|
| Reader I/O | 50-90ms | NVMe, no wait if free slot |
| Parallel offset finding | 3-5ms | ALL cores, negligible |
| Parallel decode | ~1000ms | 32 cores, ~255 segments |
| Send segments to writer | ~550ms | Moving Vec<u8> ownership |
| Writer steady state | ~0.14s at 3500 MB/s | Page cache, fast |
| Reader wait for slot | ~1500-1700ms | Blocked on slot recycle |

### Timing Feature
`cargo build --release --features timing` — instrumented stderr output.
Code in `chunk.rs` and `bin/lbunzip2.rs`, `#[cfg(feature = "timing")]`.
Segment CSV: `/tmp/lbzip2_segments.csv`.

---

## 10. Module Map

```
src/
├── lib.rs           # BLOCK_MAGIC/FINAL_MAGIC, dedicated rayon pool (LBZIP2_THREADS)
├── bitreader.rs     # 64-bit buffered reader, arbitrary bit offset, peek/consume
├── block.rs         # Single block decode — THE OPTIMIZED PATH. Thread-local tt pool.
├── block_scan.rs    # 48-bit scanner, split_boundaries_parallel, quick_verify
├── bwt.rs           # Inverse BWT (in-place T-transformation)
├── chunk.rs         # ChunkDecoder: parallel offset find + segment decode, pbzip2
├── huffman.rs       # 10-bit packed lookup + tree fallback
├── mtf.rs           # Move-to-front: fast n=0/n=1
├── parallel.rs      # In-memory parallel (small files only)
├── reader.rs        # StreamingBz2Read + ParallelBz2Read
├── stream.rs        # Sequential decoder (reference/benchmark)
└── bin/lbunzip2.rs  # CLI: 3-thread pipeline with ring buffer
```

---

## 11. Build & Test

```bash
cargo build --release                    # normal build
cargo build --release --features timing  # with timing instrumentation
cargo test                               # unit tests
cargo test --release --test decompress_bench -- --nocapture --ignored  # benchmark
cargo test --release --test stage_breakdown -- --nocapture --ignored   # hotspot analysis
LBZIP2_THREADS=8 cargo run --release --bin lbunzip2 -- input.bz2 out  # override threads
```

---

## 12. Test Data

- `test_data/hello.bz2` — 14 bytes → "Hello, World!\n"
- `test_data/liechtenstein.osm.bz2` — 5.2 MB → 60 MB (71 blocks)
- `/home/rickard/work/planet-241021.osm.bz2` — 147 GB (~1.3M mini-streams)
- `/home/rickard/work/planet_1g.bz2` — 1 GB snippet for quick tests

---

## 13. Hardware

| Machine | CPU | Cores | SMT | RAM | L2 | L3 | Storage |
|---|---|---|---|---|---|---|---|
| **odin** | Threadripper PRO 3975WX (Zen2) | 32 | OFF | 512 GB DDR4 | 16 MB | 128 MB | 2× Samsung 1735 RAID-0 XFS |
| **loki** | Ryzen 9 7900 (Zen4) | 12 | OFF | 64 GB DDR5 | 12 MB | 64 MB | Corsair MP700 Pro PCIe 5.0 |
| **laptop** | T14s 2024, Ryzen AI 360 (Zen5) | 8+SMT | ON | 64 GB DDR5 | 8 MB | 24 MB | Kioxia NVMe |

---

## 14. Common Mistakes — DO NOT DO THESE

1. **Do NOT re-analyze the codebase from scratch** — read this doc + design.md first.
2. **Do NOT optimize split_boundaries_parallel** — 3-5ms, negligible.
3. **Do NOT suggest "pipelining the scan ahead"** — 3ms vs 1000ms decode.
4. **Do NOT touch the iBWT+RLE2 decode path** — it's 1.5× faster than C, it stays.
5. **Do NOT add allocations in the hot path** — the goal is FEWER allocs, not more.
6. **Do NOT confuse parallel.rs with chunk.rs** — parallel.rs is for small files only.
7. **Do NOT forget pbzip2 streams** — FINAL_MAGIC every ~120 KB in planet files.
8. **Do NOT claim disk is the bottleneck** — NVMe reads are 50-90ms, decode is 1000ms.
9. **Do NOT suggest increasing buffer sizes as optimization** — the ring design is correct.

---

## 15. Znippy Ecosystem

Used by [osm-katana](https://github.com/Ignalina/katana-osm): bz2 → VTD XML → PBF.
Library API (`ChunkDecoder`) designed for zero-copy: caller owns buffers, decoder borrows.
End-to-end planet: 81 minutes, 309 MB/s decompressed.
