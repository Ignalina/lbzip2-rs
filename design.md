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
 │  BitReader          │  64-bit buffered, zero-copy from &[u8]
 │  → Huffman (table)  │  12-bit fast lookup, tree fallback
 │  → MTF decode       │
 │  → inverse BWT      │
 │  → RLE2             │
 └────────┬───────────┘
          │  Vec<Vec<u8>>  (ordered segments)
          ▼
    caller receives segments directly (no assembly memcpy)
```

## Optimizations

- **BitReader**: 64-bit shift register with bulk refill (vs per-bit byte lookup)
- **Huffman**: 12-bit flat lookup table, cold tree fallback for codes >12 bits
- **BWT pointer chase**: raw pointer with `unsafe` bounds elision
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
