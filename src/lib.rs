//! lbzip2-rs — Pure Rust parallel bzip2 decompressor.
//!
//! Architecture:
//!   1 reader thread fills a ChunkRevolver ring buffer (~100 MB slots)
//!   with raw compressed bzip2 data.
//!
//!   Per slot the reader does a cheap bit-scan to locate bzip2 block
//!   boundaries (magic 0x314159265359 at arbitrary bit offsets), then
//!   dispatches N sub-ranges to a worker pool.
//!
//!   Each worker independently decodes its Burrows-Wheeler blocks and
//!   produces decompressed output.  Results are reassembled in order and
//!   emitted as a streaming `Read`.

pub mod bitreader;
pub mod block;
pub mod block_scan;
pub mod bwt;
pub mod chunk;
pub mod huffman;
pub mod mtf;
pub mod par;
pub mod parallel;
pub mod reader;
pub mod stream;

/// bzip2 block magic: π digits — 0x314159265359 (48 bits, bit-aligned).
pub const BLOCK_MAGIC: u64 = 0x314159265359;

/// bzip2 end-of-stream magic: √π digits — 0x177245385090 (48 bits).
pub const FINAL_MAGIC: u64 = 0x177245385090;

// ── Parallelism ──────────────────────────────────────────────────────────────
//
// bzip2 decode is parallelised with a rayon-free scoped-thread work-stealing
// map (`par::par_map`). Worker count is sized to physical cores, overridable
// via the `LBZIP2_THREADS` env var.
