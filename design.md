al# lbzip2-rs Design

Pure Rust parallel bzip2 decompressor — library internals.

## bzip2 Stream Format

A single bzip2 stream:

```
[BZh9]  [BLOCK_MAGIC block_data]  [BLOCK_MAGIC block_data]  ...  [FINAL_MAGIC crc32] [pad]
  4B         48 bits                    48 bits                      48 bits   32 bits  0-7 bits
```

- **Stream header** (`BZhN`): 4 bytes. `N` = block size level (`1`–`9`), max block = N × 100,000 bytes.
- **Block magic**: 48-bit `0x314159265359` (digits of π). Blocks are **bit-aligned**, not byte-aligned.
- **Block data**: CRC32 (32 bits), randomised flag (1 bit), orig_ptr (24 bits), symbol bitmap,
  Huffman tables, MTF-encoded symbols. Variable length.
- **End-of-stream**: 48-bit `0x177245385090` + 32-bit stream CRC + padding to byte boundary.

### pbzip2 Concatenated Streams

Files produced by `pbzip2` (parallel bzip2 compressor) are **not** a single stream.
They are many independent bzip2 streams concatenated:

```
[BZh9][blocks...][FINAL_MAGIC][crc32][pad]  [BZh9][blocks...][FINAL_MAGIC][crc32][pad]  ...
└──────── stream 1 ────────────────────┘    └──────── stream 2 ────────────────────┘
```

A 147 GB planet OSM file has ~1.3 million mini-streams, each ~120 KB compressed.
Each mini-stream has its own header, blocks, and end-of-stream marker.

**Critical implication**: FINAL_MAGIC appears every ~120 KB, not just at EOF.
A decoder that stops at the first FINAL_MAGIC will decompress only one mini-stream.

When hitting FINAL_MAGIC during streaming decode:
1. Read 32-bit stream CRC (skip it)
2. Align to next byte boundary (0–7 padding bits)
3. Read 4-byte `BZhN` header of the next stream
4. Continue reading BLOCK_MAGIC + block data as normal

### 48-bit Block Magic False Positives

The 48-bit magic `0x314159265359` can match inside compressed data by coincidence.

**Quick verification** (73 bits, ~10 bytes after magic):
- CRC32 (32 bits) — any value
- Randomised flag (1 bit) — must be 0
- orig_ptr (24 bits) — must be < max_blocksize (900,000)
- Symbol group bitmap (16 bits) — must be nonzero

This rejects false positives almost instantly without a full block decode.

## Parallel Block Decode

### Split Boundaries

Instead of scanning the entire chunk for all block magics (expensive),
we calculate N evenly-spaced **nominal positions** and forward-scan from each:

```
|<─────────────────── 200 MB chunk ──────────────────────>|
|    ↓ split 1    ↓ split 2    ↓ split 3    ...          |
|    scan ~200KB  scan ~200KB  scan ~200KB                |
```

Each of N cores forward-scans for the next BLOCK_MAGIC, then quick-verifies
the 73-bit header. All N-1 splits run simultaneously via rayon.

Total scanning: N × ~200 KB ≈ ~3 MB for 16 cores = **~1.5%** of 200 MB.

### Segment Streaming

The chunk is divided into segments at the split boundaries.
Each segment is decoded in parallel by one Rayon thread:

```
Segment 0: [first_block ──────── split_1)
Segment 1: [split_1 ──────────── split_2)
...
Segment N: [split_N ──────────── EOF]
```

Within each segment, the thread **streams** through the bitstream:
1. Decode block at the boundary
2. Read next 48-bit magic
3. If BLOCK_MAGIC → decode next block, repeat
4. If FINAL_MAGIC → skip CRC + pad + BZhN header, continue (pbzip2 support)
5. If end of segment → stop

No scanning within segments — just follows the natural bzip2 bitstream.

## Decode Pipeline

```
compressed &[u8]
     │
     ▼
 ┌────────────────────┐
 │  block_scan         │  64-bit sliding window, 16-way unrolled per 2-byte step
 │  split_boundaries   │  N parallel forward scans + 73-bit quick verify
 └────────┬───────────┘
          │  Vec<BlockBoundary>
          ▼
 ┌────────────────────┐
 │  rayon par_iter     │  N segments decoded concurrently
 │  BitReader          │  64-bit buffered, bulk 8-byte refill, zero-copy from &[u8]
 │  → Huffman (table)  │  10-bit packed lookup, tree fallback
 │  → MTF decode       │  fast-path n=0/n=1
 │  → inverse BWT      │
 │  → RLE2 (prefetch)  │  2-step lookahead, raw pointer output
 └────────┬───────────┘
          │  Vec<Vec<u8>>  (ordered segments)
          ▼
    caller receives segments directly (no assembly memcpy)
```

## Optimizations

- **BitReader**: 64-bit shift register with bulk 8-byte refill (single load vs byte-at-a-time loop)
- **Huffman**: 10-bit packed `u16` lookup table (2 KB/table, all 6 fit L1), cold tree fallback for codes >10 bits
- **MTF**: fast-path for n=0 (no-op return) and n=1 (swap), the two most common indices
- **Decode loop**: group-of-50 inner loop — fixed tree pointer, no per-symbol counter check
- **BWT pointer chase**: 2-step prefetch lookahead hides L3 latency on the 3.6 MB random-access `tt` array
- **RLE2 output**: raw pointer writes (no Vec bounds check per byte), `memset` for repeat runs
- **Thread-local tt buffers**: reuse the 3.6 MB `tt` Vec across blocks on the same thread (no alloc/free per block)
- **Block scan**: 64-bit sliding window, 16-way unrolled per 2-byte step
- **Split verification**: 73-bit header check, not full trial-decode
- **Segment output**: `decode_chunk_segments()` returns Vec<Vec<u8>> — no single-thread assembly

## Module Map

```
src/
├── lib.rs           # Crate root, BLOCK_MAGIC/FINAL_MAGIC constants
├── bitreader.rs     # Bit-level reader, 64-bit buffer, arbitrary bit offset start
├── block.rs         # Single block decoder: Huffman, MTF, inverse BWT, RLE
├── block_scan.rs    # 48-bit magic scanner, split_boundaries, quick verify
├── chunk.rs         # ChunkDecoder: parallel segment decode, concatenated stream support
├── bwt.rs           # Inverse Burrows-Wheeler transform
├── huffman.rs       # Huffman tree decode
├── mtf.rs           # Move-to-front decoder
├── parallel.rs      # In-memory parallel approach (decompress_parallel)
├── reader.rs        # Streaming/mmap reader interfaces
└── stream.rs        # Single-stream sequential decoder
```

## Backlog / Wishes for bzip2-rs crate

Questions / wishes for the `bzip2-rs` crate author — API changes that
would have made parallel decode possible without reimplementing the decoder:

```
1. pub fn decode_block(data: &[u8], bit_offset: usize, max_blocksize: u32)
       -> Result<(Vec<u8>, usize), Error>
   — Expose single-block decode from arbitrary bit offset.
   — Return (decompressed_bytes, bits_consumed).

2. Zero-copy input: accept &[u8] + bit_offset, not impl Write.
   — For mmap / ring-buffer use cases, borrowing is essential.

3. Expose block boundary scanning or document the 48-bit bit-aligned
   magic (0x314159265359) so callers can split the stream themselves.

4. Optional: fn decode_block_into(data: &[u8], bit_offset: usize,
                                   out: &mut [u8]) -> Result<usize, Error>
   — Write directly into caller-provided buffer.
```

Without (1) and (2), parallel decode requires reimplementing the full
Huffman → MTF → BWT → RLE pipeline from scratch (which is what this crate does).

## Performance: Adapting 1996 Algorithms to Modern Hardware

The original bzip2 was written by Julian Seward in 1996 for CPUs where computation
was the bottleneck and memory was fast relative to the clock. Modern hardware has
inverted this: CPUs are wide and fast, but memory latency (100+ cycles to L3)
dominates. The algorithm is identical — these optimizations adapt the *mechanics*
to 2020s hardware.

| Optimization | Hardware shift since '96 | What we changed |
|---|---|---|
| **Thread-local buffer pool** | RAM is cheap, cores are many | Reuse 3.6 MB `tt` buffers per thread instead of alloc/free per block |
| **2-step prefetch lookahead** | L3 latency now 100+ cycles vs ~10 in '96 | Prefetch the BWT pointer chain 2 hops ahead — hide L3 latency |
| **Packed Huffman tables** | L1 cache is precious (32–48 KB) | Pack entries into `u16`, shrink from 96 KB → 12 KB — all 6 tables fit L1 |
| **64-bit bulk bitreader** | Registers are 64-bit | Load 8 bytes in one shot instead of byte-at-a-time loop |
| **Raw pointer output** | Branch prediction is deep but mispredicts are costly | Skip Vec bounds check per byte, `memset` for repeat runs |
| **MTF fast paths** | Branch prediction loves hot paths | Special-case n=0 (no-op) and n=1 (swap) — the two most common indices |
| **Group-of-50 inner loop** | Tight loops let the CPU speculate and prefetch better | Eliminate per-symbol counter check; fixed tree pointer for 50 symbols |

### Benchmark (liechtenstein.osm.bz2, 5.2 MB → 60 MB)

| Mode | Throughput | vs C libbz2 |
|---|---|---|
| C libbz2 (single-thread) | 108 MB/s | 1.0× |
| lbzip2-rs (single-thread) | 141 MB/s | 1.3× |
| lbzip2-rs (parallel, 12 threads) | 704 MB/s | 6.5× |

### Stage breakdown (single-thread, 71 blocks)

| Stage | Time | % |
|---|---|---|
| Header + bitmap + selectors + trees | 3 ms | 0.8% |
| Huffman decode + MTF + RLE1 | 105 ms | 26% |
| Inverse BWT | 35 ms | 9% |
| RLE2 + output | 260 ms | 64% |
| **Total** | **403 ms** | |

The RLE2 stage dominates because the inverse BWT traversal is a dependent pointer
chain through a 3.6 MB array — fundamentally memory-latency-bound. The 2-step
prefetch hides most of L3 latency but cannot eliminate the serial dependency.
