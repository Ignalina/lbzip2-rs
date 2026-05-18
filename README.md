hmm # lbzip2-rs

<p align="center">
  <img src="doc/media/znippys.png" alt="znippys" width="400"/>
</p>

> 🧙‍♂ Med Allfaderns visdm och blick över varje bit.

Pure Rust parallel bzip2 decompressor. No C dependencies.
Usable as a **library** (in-process, zero-copy) or as a **CLI** tool:

```bash
# Decompress any bzip2 file (including pbzip2 concatenated streams)
cargo run --release --bin lbunzip2 -- planet-241021.osm.bz2 > planet.osm
```

**What makes this crate unique:** 100% Rust, zero-copy in-process library with a lightweight parallel split method that finds block boundaries with tiny forward-scans (~500 bytes per split). The decoder uses a streaming worker-pool (reorder buffer) to avoid global barriers and is tuned for modern CPU caches (packed Huffman tables fit L1, thread-local buffers leverage L2), yielding high throughput on many-core machines.

Part of the [znippy](https://github.com/Ignalina) group of software,
designed for fast zero-copy integration with
[osm-katana](https://github.com/Ignalina/katana-osm) — the parallel
OSM-to-GeoParquet pipeline.

## Performance

### Library (in-process — Planet 1 GB slice)

Measured on odin (Threadripper PRO 32 cores, 512 GB RAM) using /home/rickard/work/planet_1g.bz2 (1 GB compressed → ~9.86 GB decompressed):

| Mode | Throughput | Notes |
|------|-----------:|-------|
| lbzip2-rs parallel (32 threads) | 1799 MB/s | 1 GB → 9.86 GB in 5.5 s (measured)


### CLI (lbunzip2)

| Test file | lbzip2-rs (measured) | Notes |
|-----------|---------------------:|-------|
| Planet 1 GB slice (→ 9.86 GB) | 5.5 s (1799 MB/s) | odin (32 cores), /home/rickard/work/planet_1g.bz2 → /dev/null
| Liechtenstein 3 MB (→ 60 MB) | 0.22 s | startup overhead |

Measured on odin (32 cores, NVMe). Use LBZIP2_THREADS to control worker count.

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

## Förklaring för Farbror Dan 🏍️🇸🇪

> *Hur fungerar den nya worker-pool-arkitekturen?*

Tänk dig **6 rum** (slottar) som återanvänds om och om igen. Varje rum
rymmer **200 MB komprimerad data**. I varje rum finns **32 väggar** — en
per målare. Vi har exakt **32 målare** (fysiska CPU-kärnor) och en
**materialare** (läs-tråden) som fyller rummen med nytt material.

När ett rum är målat klart, städas det och fylls med nytt material —
rummen **roterar** i en ring. Med 6 rum finns det alltid ett par redo
medan andra målas.

### Gamla sättet (rayon-barriär)

Alla 32 målare går in i **rum 1**. Var och en målar sin vägg.
**Men** — *ingen* får gå vidare till rum 2 förrän *alla* är klara.

Kalle blir klar efter 80 ms. Lisa behöver 220 ms (hennes vägg var
krångligare). Kalle **står och väntar i 140 ms med armarna i kors**.
Gånger 31 målare som väntar ≈ **~700 ms slösad väntetid per slot**.

```
Rum 1:  [████████████░░] alla väntar på Lisa...
Rum 2:  [              ] tomt — ingen får börja!
Rum 3:  [              ]
```

### Nya sättet (worker pool — inga barriärer)

Kalle klar med sin vägg i rum 1? **Rakt in i rum 2.** Ingen väntan.
Lisa jobbar fortfarande i rum 1 — det är lugnt, rummet skrivs till
disk först när alla 32 väggar är klara. Men målarna **slutar aldrig
jobba**.

```
Rum 1:  [████████████░░] Lisa nästan klar
Rum 2:  [██████░░░░░░░░] Kalle + 20 andra redan igång!
Rum 3:  [██░░░░░░░░░░░░] snabbaste målarna redan här
```

### Vad tjänade vi?

| | Gamla | Nya | Besparing |
|---|---|---|---|
| **Väntetid per slot** | ~700 ms | ~0 ms | **700 ms/slot** |
| **1 GB fil (5 slottar)** | 5.5 s | 5.2 s | **6% snabbare** |
| **Planet 147 GB (~735 slottar)** | ~81 min | ~73 min | **~8 min** |
| **Genomströmning** | 1 804 MB/s | 1 909 MB/s | **+105 MB/s** |

735 slottar × 700 ms barriär = **~515 s bortkastad väntetid** eliminerad.

Hemligheten: målarna **slutar aldrig röra penseln**. Ingen barriär,
ingen chef som ropar "STOPP!" mellan rummen. De 6 rummen roterar —
materialaren fyller, målarna målar, skrivaren skriver — som ett
löpande band.

*"Trettiotvå målare som aldrig vilar — det är Sleipners åtta ben
gånger fyra."* 🐴

## License

MIT OR Apache-2.0, plus the original bzip2 license (BSD-style, Julian
Seward) for the block-decode routines inspired by the C reference
implementation. See [LICENSE-BZIP2](LICENSE-BZIP2) for the full text.

