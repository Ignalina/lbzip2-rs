# lbzip2-rs Design

Pure Rust parallel bzip2 decompressor — library + CLI.

## CLI Pipeline (lbunzip2)

Three threads, zero-copy ring buffer:

```
┌──────────┐    sync_channel     ┌──────────────┐    sync_channel     ┌──────────┐
│  READER  │──(slot,len,last)──→ │  MAIN THREAD │──(Vec<u8> segs)──→ │  WRITER  │
│  thread  │                     │ carry+decode  │                     │  thread  │
└────┬─────┘                     └──────────────┘                     └──────────┘
     ↑                                  │
     └──── slot_return channel ─────────┘   (4 × 232 MB recycled buffers)
```

**Ring buffer**: 4 pre-allocated slots, each `[32 MB headroom | 200 MB chunk]`.
Slots are never freed — recycled through `slot_return` channel.

**Carry**: between chunks, the unconsumed tail (< 1 MB) is copied into the
headroom area of the next slot. The 200 MB read data stays in place — never copied.

**Per-chunk cycle**:
1. Reader fills 200 MB slot from disk (~50-90ms NVMe)
2. Main thread: carry into headroom → `decode_chunk_segments()` → recycle slot
3. Segments sent individually to writer (no assembly memcpy)

## Parallel Split + Decode

### O(N) Boundary Finding

Not a full scan. N×oversplit evenly-spaced positions, each forward-scans ~500 bytes
for the next BLOCK_MAGIC, then 73-bit quick-verify. **Total: 3-5ms for 200 MB.**

### Segment Decode

Chunk split into ~255 segments at boundaries. Each decoded by one rayon thread.
Within a segment: sequential bitstream walk (Huffman → MTF → BWT → RLE2).
Oversplit 8× enables rayon work-stealing for load balance.

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

**Thread-local tt pool**: 3.6 MB buffer reused per thread, no alloc/free per block.
**Zero heap allocs per block**: Huffman tables, selectors, bitmaps all on stack.

## bzip2 Stream Format

```
[BZh9]  [BLOCK_MAGIC block_data]...  [FINAL_MAGIC crc32] [pad]
```

- Block magic: 48-bit `0x314159265359` (π digits), **bit-aligned** (not byte-aligned)
- End-of-stream: 48-bit `0x177245385090` (√π digits) + CRC32 + byte-pad

### pbzip2 Concatenated Streams

Planet files are ~1.3M independent mini-streams concatenated (~120 KB each).
FINAL_MAGIC appears every ~120 KB, not just at EOF. Segment decoders handle
FINAL_MAGIC → skip CRC + pad + BZhN header → continue.

### False Positive Rejection

73-bit quick-verify after magic: randomised=0, orig_ptr < max_blocksize, bitmap ≠ 0.

## Module Map

```
src/
├── lib.rs           # BLOCK_MAGIC/FINAL_MAGIC, dedicated rayon pool (LBZIP2_THREADS)
├── bitreader.rs     # 64-bit buffered reader, arbitrary bit offset, peek/consume
├── block.rs         # Single block: Huffman→MTF→BWT→RLE2, thread-local tt pool
├── block_scan.rs    # 48-bit scanner, split_boundaries_parallel, quick_verify
├── bwt.rs           # Inverse Burrows-Wheeler (in-place T-transformation)
├── chunk.rs         # ChunkDecoder: parallel segment decode, pbzip2 support
├── huffman.rs       # 10-bit packed lookup + tree fallback
├── mtf.rs           # Move-to-front: fast n=0/n=1
├── parallel.rs      # In-memory parallel (small files)
├── reader.rs        # StreamingBz2Read + ParallelBz2Read (mmap)
├── stream.rs        # Sequential decoder (reference path)
└── bin/lbunzip2.rs  # CLI: 3-thread pipeline with ring buffer
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

**Planet** (147 GB, 32 cores, per-chunk timing):

| Phase | Time | Notes |
|---|---|---|
| Reader I/O | 50-90ms | NVMe |
| split_boundaries_parallel | 3-5ms | Negligible |
| Parallel decode (32 cores) | ~1000ms | 200 MB → ~1900 MB |
| Send segments to writer | ~550ms | Vec<u8> through sync_channel |

## Ideas from Claude — Streaming Worker Pool

### The Problem: `par_iter().collect()` Is a Barrier

Right now, rayon decodes all ~255 segments in parallel, but `.collect()` waits for
**every** segment to finish before **any** result goes to the writer. Measured on
odin (32 cores, planet_1g.bz2):

| Oversplit | Segments | Efficiency | Decode Wall | Send | **Total** |
|-----------|----------|------------|-------------|------|-----------|
| 4 | 127 | 89-91% | 975-1068ms | 32-72ms | **5.6s** |
| 8 (default) | 255 | 93-95% | 914-967ms | 47-66ms | **5.2s** |
| 16 | 511 | 95-97% | 926-1007ms | 88-152ms | **5.7s** |
| 32 | 1023 | 97-98% | 897-981ms | 141-185ms | **5.8s** |

More oversplit → better efficiency (less tail-wait) but **send phase explodes**
because more segments means more channel sends. The sweet spot is oversplit=8.

The real issue: between decode finishing and the next decode starting, all 32
cores sit idle during carry + send (~60-150ms). That's 6-15% of wall time with
zero CPU. On htop you see 100% → 0% → 100% — the sawtooth.

### The Idea: Stream Segments Out During Decode

Instead of:
```
[=== all 32 cores decode 255 segments ===][--- send 255 results ---][=== next chunk ===]
                                           ^ 32 cores idle here
```

Do this:
```
[=== 32 cores decode, finished segments flow to writer as they complete ===][=== next chunk ===]
                                                                            ^ no gap
```

Each core finishes a segment → drops it into an **ordered output queue** →
writer picks it up immediately. No barrier. Decode and send overlap completely.

Implementation: replace `par_iter().collect()` with N persistent worker threads.
Each worker loops: grab next segment index from an atomic counter → decode →
put result into a slot array at that index → signal the drain thread.
A drain thread walks the slot array in order, sending completed segments to
the writer as soon as they're ready (wait only if segment i isn't done yet).

The drain thread is basically: `for i in 0..n_segments { wait(slot[i]); send(slot[i]); }`.
Like a reorder buffer in a CPU pipeline — results arrive out of order, leave in order.

### What This Buys

- **Zero idle gap** between chunks — send overlaps with decode
- **Writer starts earlier** — first segment arrives after ~30ms instead of ~950ms
- **Eliminates the collect() barrier** — fast segments don't wait for slow ones
- **Natural backpressure** — if writer is slow, drain thread blocks, workers keep
  decoding into remaining slots

### What It Doesn't Change

- Total CPU work per block is the same — this just removes idle gaps between chunks
- Won't help much on 8-core laptop (already 90% utilization, small gaps)

### Risk

- Ordered output is critical — segments must reach the writer in order
- More complex than rayon par_iter — but not rocket science, it's a reorder buffer
- Need to handle the pre-allocated output buffer idea at the same time to avoid
  255 × 7 MB mallocs per chunk (separate concern, but natural fit)

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

---

## History

### Earlier Benchmark Numbers (Loki — Ryzen 9 7900, 12 cores)

| Mode | Throughput | vs C libbz2 |
|---|---|---|
| C libbz2 (single-thread) | 108 MB/s | 1.0× |
| lbzip2-rs (single-thread) | 143 MB/s | 1.3× |
| lbzip2-rs (parallel, 12 threads) | 731 MB/s | 6.8× |

Stage breakdown (Loki, single-thread):

| Stage | Time | % |
|---|---|---|
| Header + bitmap + selectors + trees | 3 ms | 0.8% |
| Huffman decode + MTF + RLE1 | 98 ms | 25% |
| Inverse BWT | 35 ms | 9% |
| RLE2 + output | 249 ms | 65% |
| **Total** | **384 ms** | |

### Wishes for bzip2-rs Crate

API changes that would have made parallel decode possible without
reimplementing the decoder:

1. `decode_block(data: &[u8], bit_offset, max_blocksize)` — single-block from arbitrary offset
2. Zero-copy input: `&[u8]` + bit_offset, not `impl Write`
3. Expose block boundary scanning or document the 48-bit magic
4. `decode_block_into()` — write into caller buffer

Without (1) and (2), parallel decode requires reimplementing the full pipeline.
