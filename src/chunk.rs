//! Chunk-level parallel bzip2 decoder.
//!
//! Designed for zero-copy integration with ChunkRevolver: the caller
//! reads ~100 MB of compressed data into a ring buffer slot, then
//! passes the `&[u8]` directly to `ChunkDecoder::decode_chunk()`.
//!
//! The decoder scans for block boundaries, parallel-decodes all
//! complete blocks, and returns:
//!   - `decompressed`: the concatenated output (~800–1000 MB)
//!   - `consumed`:     how many bytes were fully decoded
//!
//! The caller carries `data[consumed..]` into the next slot.

use crate::bitreader::BitReader;
use crate::block::{self, BlockError};
use crate::block_scan;
use crate::{BLOCK_MAGIC, FINAL_MAGIC};

/// Result of splitting a chunk into segment boundaries.
pub struct ChunkSplit {
    /// Segment start boundaries (one per segment found).
    pub segment_starts: Vec<block_scan::BlockBoundary>,
    /// Number of segments to decode (all if is_last, else n-1).
    pub decode_segments: usize,
    /// Bytes consumed from the data (for carry computation).
    pub consumed: usize,
}

/// Split a chunk of compressed data into segment boundaries.
///
/// `n_segments` is typically n_cores (one segment per worker).
/// Uses parallel scanning (scoped-thread `par_map`) for speed.
///
/// Returns `None` if no block boundary found in data.
pub fn split_chunk(
    data: &[u8],
    n_segments: usize,
    max_blocksize: u32,
    is_last: bool,
) -> Option<ChunkSplit> {
    let first_block = block_scan::find_next_block(data, 0)?;

    let splits = block_scan::split_boundaries_parallel(data, n_segments, max_blocksize);

    let mut segment_starts = Vec::with_capacity(n_segments);
    segment_starts.push(first_block);
    for s in &splits {
        if segment_starts.last().map_or(true, |prev: &block_scan::BlockBoundary| {
            prev.bit_offset != s.bit_offset
        }) {
            segment_starts.push(*s);
        }
    }

    let n = segment_starts.len();
    let decode_segments = if is_last {
        n
    } else if n > 1 {
        n - 1
    } else {
        return None;
    };

    let consumed = if decode_segments < n {
        segment_starts[decode_segments].byte_offset()
    } else {
        data.len()
    };

    Some(ChunkSplit { segment_starts, decode_segments, consumed })
}

/// Decode a single segment of compressed bzip2 data.
///
/// Reads blocks from `start_bit` (after the 48-bit BLOCK_MAGIC) up to `end_bit`.
/// Handles pbzip2 concatenated streams (FINAL_MAGIC boundaries).
///
/// Zero per-block allocation: decodes directly into the output buffer
/// using `decode_block_into()`. Only the segment Vec grows (amortised).
///
/// Returns the decompressed output as a Vec<u8>.
pub fn decode_segment(
    data: &[u8],
    start_bit: u64,
    end_bit: u64,
    max_blocksize: u32,
) -> Vec<u8> {
    let total_bits = data.len() as u64 * 8;
    // Max output per block: blocksize + 25% for RLE2 expansion.
    let block_cap = max_blocksize as usize + max_blocksize as usize / 4;
    let mut output = Vec::new();

    let mut reader = BitReader::from_bit_offset(data, (start_bit + 48) as usize);
    match decode_block_into_vec(&mut output, &mut reader, max_blocksize, block_cap) {
        Ok(_) => {}
        Err(_) => return output,
    }

    loop {
        let pos = reader.position() as u64;
        if pos + 48 > total_bits || pos >= end_bit {
            break;
        }
        let magic = match reader.read_u64(48) {
            Some(v) => v,
            None => break,
        };
        if magic == BLOCK_MAGIC {
            if decode_block_into_vec(&mut output, &mut reader, max_blocksize, block_cap).is_err() {
                break;
            }
        } else if magic == FINAL_MAGIC {
            if reader.read_u32(32).is_none() { break; }
            let p = reader.position();
            let pad = (8 - (p % 8)) % 8;
            if pad > 0 { BitReader::skip(&mut reader, pad); }
            match reader.read_u32(32) {
                Some(h) => {
                    let b = h.to_be_bytes();
                    if &b[..3] != b"BZh" {
                        break;
                    }
                }
                None => break,
            }
        } else {
            break;
        }
    }

    output
}

/// Decode one block directly into the tail of `output`, avoiding a temporary Vec.
#[inline]
fn decode_block_into_vec(
    output: &mut Vec<u8>,
    reader: &mut BitReader<'_>,
    max_blocksize: u32,
    block_cap: usize,
) -> Result<(), block::BlockError> {
    output.reserve(block_cap);
    let cur_len = output.len();
    // Safety: we just reserved block_cap bytes; the spare capacity is valid
    // uninitialised memory that decode_block_into will write into.
    unsafe { output.set_len(cur_len + block_cap); }
    match block::decode_block_into(reader, max_blocksize, &mut output[cur_len..]) {
        Ok(written) => {
            unsafe { output.set_len(cur_len + written); }
            Ok(())
        }
        Err(e) => {
            unsafe { output.set_len(cur_len); }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn max_blocksize(data: &[u8]) -> u32 {
        100_000 * (data[3] - b'0') as u32
    }

    #[test]
    fn chunk_hello() {
        let data = include_bytes!("../test_data/hello.bz2");
        let split = split_chunk(data, 4, max_blocksize(data), true).unwrap();
        let total_bits = data.len() as u64 * 8;
        let mut output = Vec::new();
        for i in 0..split.decode_segments {
            let start = split.segment_starts[i].bit_offset;
            let end = if i + 1 < split.segment_starts.len() {
                split.segment_starts[i + 1].bit_offset
            } else {
                total_bits
            };
            output.extend_from_slice(&decode_segment(data, start, end, max_blocksize(data)));
        }
        assert_eq!(&output, b"Hello, World!\n");
    }

    #[test]
    fn chunk_liechtenstein() {
        let data = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        let split = split_chunk(data, n, max_blocksize(data), true).unwrap();
        let total_bits = data.len() as u64 * 8;
        let mut output = Vec::new();
        for i in 0..split.decode_segments {
            let start = split.segment_starts[i].bit_offset;
            let end = if i + 1 < split.segment_starts.len() {
                split.segment_starts[i + 1].bit_offset
            } else {
                total_bits
            };
            output.extend_from_slice(&decode_segment(data, start, end, max_blocksize(data)));
        }
        let reference = crate::stream::decompress(data).unwrap();
        assert_eq!(output.len(), reference.len());
        assert_eq!(output, reference);
    }

    #[test]
    fn chunk_split_simulation() {
        let data = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let mbs = max_blocksize(data);
        let mid = data.len() / 2;
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);

        let split1 = split_chunk(&data[..mid], n, mbs, false).unwrap();
        let consumed1 = split1.consumed;
        assert!(consumed1 <= mid);

        let total_bits1 = mid as u64 * 8;
        let mut out1 = Vec::new();
        for i in 0..split1.decode_segments {
            let start = split1.segment_starts[i].bit_offset;
            let end = if i + 1 < split1.segment_starts.len() {
                split1.segment_starts[i + 1].bit_offset
            } else {
                total_bits1
            };
            out1.extend_from_slice(&decode_segment(&data[..mid], start, end, mbs));
        }
        assert!(!out1.is_empty());

        let chunk2 = &data[consumed1..];
        let split2 = split_chunk(chunk2, n, mbs, true).unwrap();
        let total_bits2 = chunk2.len() as u64 * 8;
        let mut out2 = Vec::new();
        for i in 0..split2.decode_segments {
            let start = split2.segment_starts[i].bit_offset;
            let end = if i + 1 < split2.segment_starts.len() {
                split2.segment_starts[i + 1].bit_offset
            } else {
                total_bits2
            };
            out2.extend_from_slice(&decode_segment(chunk2, start, end, mbs));
        }

        let mut combined = out1;
        combined.extend_from_slice(&out2);
        let reference = crate::stream::decompress(data).unwrap();
        assert_eq!(combined.len(), reference.len());
        assert_eq!(combined, reference);
    }
}
