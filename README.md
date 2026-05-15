# lbzip2-rs

<p align="center">
  <img src="doc/media/znippys.png" alt="znippys" width="400"/>
</p>

> 🧙‍♂ Med Allfaderns visdom, kompression och korruptionsskydd.
> ⚡ Med hans blick över varje bit.

Pure Rust parallel bzip2 decompressor. No C dependencies.
Usable as a **library** or as a **CLI** tool:

```bash
# Decompress any bzip2 file (including pbzip2 concatenated streams)
cargo run --release --bin lbunzip2 -- planet-241021.osm.bz2 > planet.osm
```

**What makes this crate unique:** 100 % Rust (no C/FFI), in-process,
zero-copy, *and* parallel block-boundary scanning — splitting a chunk
across *N* cores is **O(N)**, not O(n) where *n* is the raw byte count
(e.g. 200 MB per chunk). Each core only scans its own 200 MB / N slice
for the 48-bit magic, so with 12 cores the work per core is ~17 MB
instead of a single thread walking all 200 MB.

Part of the [znippy](https://github.com/Ignalina) group of software,
designed for fast zero-copy integration with
[osm-katana](https://github.com/Ignalina/katana-osm) — the parallel
OSM-to-GeoParquet pipeline.

## Why

- **In-process** — no pipe, no process spawn. Decompressed segments go
  straight into the caller's memory.
- **Shared thread pool** — the rayon pool is shared with the host
  application (e.g. VTD XML parse + PBF encode). No thread contention.
- **Zero dependency on C libbz2** — builds anywhere `rustc` does.

## Performance

| Test file | C lbzip2 | lbzip2-rs | Gap |
|-----------|----------|-----------|-----|
| Planet 1 GB slice (→ ~10 GB) | 40.6 s | 42.4 s | 4% slower |
| Liechtenstein 3 MB (→ 60 MB) | 0.15 s | 0.22 s | startup |
Within **4 %** of C lbzip2 on real workloads (8-core / 16-thread machine).
Handles pbzip2 concatenated streams natively.

**Current state:** the block-level decompression (Huffman → MTF → inverse
BWT → RLE) is heavily (ai cloned) inspired by Julian Seward's original C bzip2
library. This crate therefore includes the bzip2 license (BSD-style)
alongside MIT / Apache-2.0.



## Usage

```rust
use lbzip2::chunk::ChunkDecoder;

let data: &[u8] = /* compressed chunk including BZhN header */;
let decoder = ChunkDecoder::from_header(&data[..4])?;

// Returns segments separately — no giant memcpy
let (segments, consumed) = decoder.decode_chunk_segments(data, true)?;
for seg in &segments {
    // each seg is a Vec<u8> of decompressed data, in order
}
```

Single-stream sequential API also available:

```rust
let output = lbzip2::stream::decompress(&compressed)?;
```

## Backlog

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

## License

MIT OR Apache-2.0, plus the original bzip2 license (BSD-style, Julian
Seward) for the block-decode routines derived from the C reference
implementation. See [LICENSE-BZIP2](LICENSE-BZIP2) for the full text.

