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

use rayon::prelude::*;

use crate::bitreader::BitReader;
use crate::block::{self, BlockError};
use crate::block_scan;
use crate::{BLOCK_MAGIC, FINAL_MAGIC};

/// Stateful chunk decoder — holds bzip2 stream parameters.
pub struct ChunkDecoder {
    max_blocksize: u32,
}

impl ChunkDecoder {
    /// Create a decoder from the bzip2 stream header (first 4 bytes: `BZhN`).
    ///
    /// Call this once with the first 4 bytes of the file, then use
    /// `decode_chunk()` repeatedly on subsequent compressed chunks.
    pub fn from_header(header: &[u8]) -> Result<Self, BlockError> {
        if header.len() < 4 {
            return Err(BlockError("header too short"));
        }
        if &header[..2] != b"BZ" {
            return Err(BlockError("bad bzip2 signature"));
        }
        if header[2] != b'h' {
            return Err(BlockError("only huffman bzip2 supported"));
        }
        let level = header[3];
        if !(b'1'..=b'9').contains(&level) {
            return Err(BlockError("invalid bzip2 block size level"));
        }
        Ok(Self {
            max_blocksize: 100_000 * (level - b'0') as u32,
        })
    }

    /// Like `decode_chunk`, but returns each segment's output separately.
    ///
    /// Avoids the single-threaded assembly of a giant Vec — the caller
    /// can send each segment to the writer independently.
    ///
    /// Returns `(segments, bytes_consumed)`.
    pub fn decode_chunk_segments(
        &self,
        data: &[u8],
        is_last: bool,
    ) -> Result<(Vec<Vec<u8>>, usize), BlockError> {
        let first_block = match block_scan::find_next_block(data, 0) {
            Some(b) => b,
            None => return Ok((Vec::new(), 0)),
        };

        let n_threads = crate::thread_pool().current_num_threads();
        // Oversplit: more segments than cores lets rayon work-steal for balance.
        // Tunable via LBZIP2_OVERSPLIT env var (default 4).
        let oversplit: usize = std::env::var("LBZIP2_OVERSPLIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        let n_splits = n_threads * oversplit;
        let max_bs = self.max_blocksize;
        let total_bits = data.len() as u64 * 8;

        // ── Parallel: find quick-verified split boundaries ────────────
        #[cfg(feature = "timing")]
        let t0 = std::time::Instant::now();

        let splits = crate::thread_pool().install(||
            block_scan::split_boundaries_parallel(data, n_splits, max_bs)
        );

        #[cfg(feature = "timing")]
        eprintln!(
            "[timing] split_boundaries_parallel: {} splits in {:.3}ms  (chunk {:.1} MB, {} threads, {}x oversplit)",
            splits.len(),
            t0.elapsed().as_secs_f64() * 1000.0,
            data.len() as f64 / (1024.0 * 1024.0),
            n_threads,
            n_splits / n_threads,
        );

        let mut segment_starts = Vec::with_capacity(n_threads);
        segment_starts.push(first_block);
        for s in &splits {
            if segment_starts.last().map_or(true, |prev: &block_scan::BlockBoundary| {
                prev.bit_offset != s.bit_offset
            }) {
                segment_starts.push(*s);
            }
        }

        let n_segments = segment_starts.len();

        let decode_segments = if is_last {
            n_segments
        } else if n_segments > 1 {
            n_segments - 1
        } else {
            return Ok((Vec::new(), 0));
        };

        let segment_end = |i: usize| -> u64 {
            if i + 1 < n_segments {
                segment_starts[i + 1].bit_offset
            } else {
                total_bits
            }
        };

        // ── Parallel decode — one thread per segment ────────────────────
        let results: Vec<(Vec<u8>, u64, u64, f64)> = crate::thread_pool().install(|| {
            (0..decode_segments)
            .into_par_iter()
            .map(|i| {
                #[cfg(feature = "timing")]
                let t_seg = std::time::Instant::now();

                let start_bit = segment_starts[i].bit_offset + 48;
                let end_bit = segment_end(i);
                let comp_bits = end_bit.saturating_sub(segment_starts[i].bit_offset);
                let mut output = Vec::new();

                let mut reader = BitReader::from_bit_offset(data, start_bit as usize);
                let blk = match block::decode_block(&mut reader, max_bs) {
                    Ok(b) => b,
                    Err(_) => {
                        let _ms = 0.0f64;
                        #[cfg(feature = "timing")]
                        let _ms = t_seg.elapsed().as_secs_f64() * 1000.0;
                        return (output, comp_bits, 0, _ms);
                    }
                };
                output.extend_from_slice(&blk);

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
                        match block::decode_block(&mut reader, max_bs) {
                            Ok(blk) => output.extend_from_slice(&blk),
                            Err(_) => break,
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

                let out_len = output.len() as u64;
                let _ms = 0.0f64;
                #[cfg(feature = "timing")]
                let _ms = t_seg.elapsed().as_secs_f64() * 1000.0;
                (output, comp_bits, out_len, _ms)
            })
            .collect()
        });

        #[cfg(feature = "timing")]
        {
            use std::io::Write;
            static ONCE: std::sync::Once = std::sync::Once::new();
            static SEG_FILE: std::sync::Mutex<Option<std::fs::File>> = std::sync::Mutex::new(None);
            ONCE.call_once(|| {
                let mut f = std::fs::File::create("/tmp/lbzip2_segments.csv").unwrap();
                writeln!(f, "chunk,segment,comp_kb,decomp_kb,ms").unwrap();
                *SEG_FILE.lock().unwrap() = Some(f);
            });
            static CHUNK_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let cid = CHUNK_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(ref mut f) = *SEG_FILE.lock().unwrap() {
                for (i, (_seg, comp_bits, decomp_bytes, ms)) in results.iter().enumerate() {
                    writeln!(f, "{},{},{:.1},{:.1},{:.2}",
                        cid, i,
                        *comp_bits as f64 / 8.0 / 1024.0,
                        *decomp_bytes as f64 / 1024.0,
                        ms,
                    ).unwrap();
                }
            }
        }

        let segments: Vec<Vec<u8>> = results.into_iter().map(|(data, _, _, _)| data).collect();

        let consumed = if decode_segments < n_segments {
            segment_starts[decode_segments].byte_offset()
        } else {
            data.len()
        };

        Ok((segments, consumed))
    }

    /// Decode all complete bzip2 blocks in `data`.
    ///
    /// `data` is a raw slice of compressed bzip2 data (may include the
    /// 4-byte header on first call, or start mid-stream on subsequent calls).
    ///
    /// Returns `(decompressed_output, bytes_consumed)`.
    /// - `decompressed_output`: concatenated decoded blocks, in order.
    /// - `bytes_consumed`: how many bytes of `data` were fully decoded.
    ///   The caller must carry `data[bytes_consumed..]` into the next chunk.
    ///
    /// If `is_last` is true, all blocks are decoded (even the last one).
    /// Otherwise the last block is skipped (it may be incomplete).
    pub fn decode_chunk(
        &self,
        data: &[u8],
        is_last: bool,
    ) -> Result<(Vec<u8>, usize), BlockError> {
        let (segments, consumed) = self.decode_chunk_segments(data, is_last)?;
        let total_len: usize = segments.iter().map(|s| s.len()).sum();
        let mut output = Vec::with_capacity(total_len);
        for seg in segments {
            output.extend_from_slice(&seg);
        }
        Ok((output, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_hello() {
        let data = include_bytes!("../test_data/hello.bz2");
        let decoder = ChunkDecoder::from_header(data).unwrap();
        let (output, consumed) = decoder.decode_chunk(data, true).unwrap();
        assert_eq!(&output, b"Hello, World!\n");
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn chunk_liechtenstein() {
        let data = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let decoder = ChunkDecoder::from_header(data).unwrap();
        let (output, _consumed) = decoder.decode_chunk(data, true).unwrap();

        let reference = crate::stream::decompress(data).unwrap();
        assert_eq!(output.len(), reference.len());
        assert_eq!(output, reference);
    }

    #[test]
    fn chunk_split_simulation() {
        // Simulate chunked reading: split liechtenstein into two halves.
        let data = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let decoder = ChunkDecoder::from_header(data).unwrap();

        let mid = data.len() / 2;

        // First chunk: decode what's complete, get carry
        let (out1, consumed1) = decoder.decode_chunk(&data[..mid], false).unwrap();
        assert!(consumed1 <= mid);
        assert!(!out1.is_empty(), "should decode some blocks from first half");

        // Second chunk: carry + rest
        let mut chunk2 = Vec::new();
        chunk2.extend_from_slice(&data[consumed1..]);
        let (out2, _consumed2) = decoder.decode_chunk(&chunk2, true).unwrap();

        // Combined output must match full decode
        let mut combined = out1;
        combined.extend_from_slice(&out2);

        let reference = crate::stream::decompress(data).unwrap();
        assert_eq!(combined.len(), reference.len());
        assert_eq!(combined, reference);
    }
}
