//! Parallel bzip2 decompression — decode N blocks concurrently.
//!
//! Uses `block_scan::find_all_blocks()` to locate every block boundary,
//! then decodes all blocks in parallel (scoped threads). Results are concatenated
//! in order.

use crate::bitreader::BitReader;
use crate::block::{self, BlockError};
use crate::block_scan;
use crate::BLOCK_MAGIC;
use crate::FINAL_MAGIC;

/// Decompress a complete bzip2 stream using parallel block decode.
///
/// `data` must be a complete bzip2 stream (header + blocks + EOS).
/// Blocks are decoded concurrently on scoped worker threads.
pub fn decompress_parallel(data: &[u8]) -> Result<Vec<u8>, BlockError> {
    // ── Parse stream header ─────────────────────────────────────────────
    if data.len() < 4 {
        return Err(BlockError("input too short for bzip2 header"));
    }
    if &data[..2] != b"BZ" {
        return Err(BlockError("bad bzip2 signature"));
    }
    if data[2] != b'h' {
        return Err(BlockError("only huffman bzip2 supported"));
    }
    let level = data[3];
    if !(b'1'..=b'9').contains(&level) {
        return Err(BlockError("invalid bzip2 block size level"));
    }
    let max_blocksize = 100_000 * (level - b'0') as u32;

    // ── Scan for all block boundaries ───────────────────────────────────
    // Start scanning after the 4-byte header (bit 32).
    let boundaries = block_scan::find_all_blocks(data);

    if boundaries.is_empty() {
        // Might be an empty file or just EOS marker
        return Ok(Vec::new());
    }

    // ── Parallel decode ─────────────────────────────────────────────────
    let results: Vec<Result<Vec<u8>, BlockError>> = crate::par::par_map(boundaries.len(), |bi| {
            let boundary = &boundaries[bi];
            // Position reader right after the 48-bit block magic
            let bit_after_magic = boundary.bit_offset + 48;
            let mut reader = BitReader::from_bit_offset(data, bit_after_magic as usize);
            block::decode_block(&mut reader, max_blocksize)
    });

    // ── Assemble output in order ────────────────────────────────────────
    let mut total_size = 0usize;
    for r in &results {
        match r {
            Ok(v) => total_size += v.len(),
            Err(e) => return Err(BlockError(e.0)),
        }
    }

    let mut output = Vec::with_capacity(total_size);
    for r in results {
        output.extend_from_slice(&r?);
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_hello() {
        let compressed = include_bytes!("../test_data/hello.bz2");
        let output = decompress_parallel(compressed).unwrap();
        assert_eq!(&output, b"Hello, World!\n");
    }

    #[test]
    fn parallel_liechtenstein() {
        let compressed = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let output = decompress_parallel(compressed).unwrap();
        // Compare with sequential decode
        let sequential = crate::stream::decompress(compressed).unwrap();
        assert_eq!(output.len(), sequential.len(), "size mismatch");
        assert_eq!(output, sequential, "content mismatch");
    }

    #[test]
    fn parallel_matches_sequential() {
        let compressed = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let par = decompress_parallel(compressed).unwrap();
        let seq = crate::stream::decompress(compressed).unwrap();
        assert_eq!(par, seq);
    }
}
