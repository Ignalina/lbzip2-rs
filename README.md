# lbzip2-rs

<p align="center">
  <img src="doc/media/znippys.png" alt="znippys" width="400"/>
</p>

> 🧙‍♂ Med Allfaderns visdom, kompression och korruptionsskydd.
> ⚡ Med hans blick över varje bit.

Pure Rust parallel bzip2 decompressor. No C dependencies.
Usable as a **library** (in-process, zero-copy) or as a **CLI** tool:

```bash
# Decompress any bzip2 file (including pbzip2 concatenated streams)
cargo run --release --bin lbunzip2 -- planet-241021.osm.bz2 > planet.osm
```

**What makes this crate unique:** 100 % Rust (no C/FFI), in-process,
zero-copy, *and* parallel block-boundary scanning — splitting a chunk
across *N* cores is **O(N)**, not O(n) where *n* is the raw byte count
(e.g. 200 MB per chunk). Each core only scans ~500 bytes forward from
its split point, so with 16 cores the total scan is ~8 KB for a 200 MB
chunk. 4× oversplit lets rayon work-steal across 64 segments, eliminating
core idle time from uneven block sizes.

Part of the [znippy](https://github.com/Ignalina) group of software,
designed for fast zero-copy integration with
[osm-katana](https://github.com/Ignalina/katana-osm) — the parallel
OSM-to-GeoParquet pipeline.

## Performance

### Decompression (lbunzip2 CLI vs C lbzip2)

| Test file | C lbzip2 | lbzip2-rs | |
|-----------|----------|-----------|---|
| Planet 1 GB slice (→ 9.86 GB) | 30.5 s (323 MB/s) | **30.3 s (325 MB/s)** | **0.6% faster** |
| Liechtenstein 3 MB (→ 60 MB) | 0.15 s | 0.22 s | startup overhead |

8-core / 16-thread, NVMe, `/dev/null` output, 3-run average.

### End-to-end: Planet bz2 → PBF (osm-katana)

| | |
|---|---|
| Input | 147 GB planet-241021.osm.bz2 |
| Output | 68 GB planet-241021.osm.pbf |
| Time | **81 minutes** |
| Elements | 10.5 billion |
| Throughput | 309 MB/s decompressed XML |

Full pipeline: bz2 decompress → VTD XML parse → PBF encode, 15 workers.

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

Single-stream sequential API:

```rust
let output = lbzip2::stream::decompress(&compressed)?;
```

## License

MIT OR Apache-2.0, plus the original bzip2 license (BSD-style, Julian
Seward) for the block-decode routines inspired by the C reference
implementation. See [LICENSE-BZIP2](LICENSE-BZIP2) for the full text.

