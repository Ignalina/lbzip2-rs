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
pub mod parallel;
pub mod reader;
pub mod stream;

/// bzip2 block magic: π digits — 0x314159265359 (48 bits, bit-aligned).
pub const BLOCK_MAGIC: u64 = 0x314159265359;

/// bzip2 end-of-stream magic: √π digits — 0x177245385090 (48 bits).
pub const FINAL_MAGIC: u64 = 0x177245385090;

// ── Dedicated thread pool ────────────────────────────────────────────────────

use std::sync::OnceLock;

static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

/// Get (or create) lbzip2's dedicated rayon thread pool.
///
/// Sized to the number of physical cores — separate from the caller's
/// global rayon pool so bzip2 decode doesn't compete with VTD parse,
/// PBF encode, etc.
///
/// Override with `LBZIP2_THREADS` env var.
pub fn thread_pool() -> &'static rayon::ThreadPool {
    POOL.get_or_init(|| {
        let n = std::env::var("LBZIP2_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
            });
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .thread_name(|i| format!("lbzip2-{i}"))
            .build()
            .expect("failed to create lbzip2 thread pool")
    })
}
