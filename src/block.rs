//! bzip2 block decoder.
//!
//! Decodes a single bzip2 compressed block given a `BitReader` positioned
//! right after the 48-bit block magic (0x314159265359).
//!
//! Designed for parallel use: each worker gets a byte slice + bit offset,
//! decodes independently, returns decompressed `Vec<u8>`.

use crate::bitreader::BitReader;
use crate::bwt;
use crate::huffman::HuffmanTree;
use crate::mtf::MtfDecoder;

use std::cell::RefCell;

/// Maximum block size: 9 × 100,000 bytes.
const MAX_BLOCKSIZE: usize = 900_000;

thread_local! {
    static TT_BUF: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

/// Take a `tt` buffer from the thread-local pool, avoiding repeated heap allocation.
fn take_tt_buffer(capacity: usize) -> Vec<u32> {
    TT_BUF.with(|cell| {
        let mut slot = cell.borrow_mut();
        let mut buf = std::mem::take(&mut *slot);
        buf.clear();
        if buf.capacity() < capacity {
            buf.reserve(capacity - buf.len());
        }
        buf
    })
}

/// Return a `tt` buffer to the thread-local pool for reuse.
fn return_tt_buffer(buf: Vec<u32>) {
    TT_BUF.with(|cell| {
        *cell.borrow_mut() = buf;
    });
}

/// Error type for block decoding.
#[derive(Debug)]
pub struct BlockError(pub &'static str);

impl std::fmt::Display for BlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bzip2 block error: {}", self.0)
    }
}

impl std::error::Error for BlockError {}

/// Decode one bzip2 block.
///
/// `reader` must be positioned right after the 48-bit block magic.
/// `max_blocksize` comes from the stream header (100_000 × blocksize_level).
///
/// Returns the fully decompressed block data.
pub fn decode_block(reader: &mut BitReader<'_>, max_blocksize: u32) -> Result<Vec<u8>, BlockError> {
    // ── Block header ──────────────────────────────────────────────────────
    let _crc = reader.read_u32(32)
        .ok_or(BlockError("block CRC truncated"))?;

    let randomised = reader.read_bit()
        .ok_or(BlockError("randomised flag truncated"))?;
    if randomised {
        return Err(BlockError("randomised blocks not supported"));
    }

    let orig_ptr = reader.read_u32(24)
        .ok_or(BlockError("orig_ptr truncated"))? as usize;

    // ── Symbol bitmap (which bytes appear in this block) ──────────────────
    let mut used_bytes: Vec<u8> = Vec::new();

    // 16 range flags: each covers 16 byte values.
    let mut ranges_present = [false; 16];
    for range in &mut ranges_present {
        *range = reader.read_bit()
            .ok_or(BlockError("symbol range truncated"))?;
    }

    for (range_idx, &present) in ranges_present.iter().enumerate() {
        if !present { continue; }
        for sub in 0..16u8 {
            if reader.read_bit().ok_or(BlockError("symbol bitmap truncated"))? {
                used_bytes.push(range_idx as u8 * 16 + sub);
            }
        }
    }

    if used_bytes.is_empty() {
        return Err(BlockError("no symbols in block"));
    }

    let n_symbols = used_bytes.len() + 2; // +2 for RUNA, RUNB; EOB = n_symbols - 1

    // ── Huffman table selectors ───────────────────────────────────────────
    let n_groups = reader.read_u8(3)
        .ok_or(BlockError("huffman groups truncated"))?;
    if n_groups < 2 || n_groups > 6 {
        return Err(BlockError("invalid number of huffman groups"));
    }

    let n_selectors = reader.read_u16(15)
        .ok_or(BlockError("selectors_used truncated"))? as usize;

    let mut selectors = Vec::with_capacity(n_selectors);
    let mut sel_mtf = MtfDecoder::new();
    for _ in 0..n_selectors {
        let mut trees = 0u8;
        while reader.read_bit().ok_or(BlockError("selector bit truncated"))? {
            trees += 1;
            if trees >= n_groups {
                return Err(BlockError("selector tree index too large"));
            }
        }
        selectors.push(sel_mtf.decode(trees));
    }

    // ── Huffman code lengths → trees ──────────────────────────────────────
    let mut trees: Vec<HuffmanTree> = Vec::with_capacity(n_groups as usize);
    for _ in 0..n_groups {
        let mut length = reader.read_u8(5)
            .ok_or(BlockError("huffman start length truncated"))? as i32;
        let mut lengths = Vec::with_capacity(n_symbols);

        for _ in 0..n_symbols {
            loop {
                if length < 1 || length > 20 {
                    return Err(BlockError("huffman code length out of range"));
                }
                if !reader.read_bit().ok_or(BlockError("length adjust bit1 truncated"))? {
                    break;
                }
                if reader.read_bit().ok_or(BlockError("length adjust bit2 truncated"))? {
                    length -= 1;
                } else {
                    length += 1;
                }
            }
            lengths.push(length as u8);
        }

        trees.push(HuffmanTree::from_lengths(&lengths)
            .map_err(|_| BlockError("invalid huffman tree"))?);
    }

    // ── Huffman decode → MTF + RLE1 decode → tt array ─────────────────────
    let mut tt: Vec<u32> = take_tt_buffer(max_blocksize as usize);
    let mut c = [0u32; 256]; // byte frequency counts for BWT

    // Build MTF decoder with the actual used-byte alphabet.
    let mut byte_symbols = [0u8; 256];
    byte_symbols[..used_bytes.len()].copy_from_slice(&used_bytes);
    let mut mtf = MtfDecoder::with_symbols(byte_symbols);

    let mut sel_idx: usize = 0;
    let mut current_tree = trees.get(
        *selectors.first().ok_or(BlockError("no selectors"))? as usize
    ).ok_or(BlockError("selector out of range"))?;

    let mut repeat: u32 = 0;
    let mut repeat_power: u32 = 0;

    let eob_symbol = (n_symbols - 1) as u16;

    'outer: loop {
        for _ in 0..50 {
            let sym = current_tree.decode(reader)
                .ok_or(BlockError("huffman bitstream truncated"))?;

            // RUNA (0) or RUNB (1): run-length encoding.
            if sym < 2 {
                if repeat == 0 {
                    repeat_power = 1;
                }
                repeat += repeat_power << sym;
                repeat_power <<= 1;

                if repeat as usize > MAX_BLOCKSIZE {
                    return Err(BlockError("repeat count too large"));
                }
                continue;
            }

            // Flush pending run.
            if repeat > 0 {
                let b = mtf.first();
                if tt.len() + repeat as usize > max_blocksize as usize {
                    return Err(BlockError("data exceeds block size"));
                }
                let new_len = tt.len() + repeat as usize;
                tt.resize(new_len, u32::from(b));
                c[b as usize] += repeat;
                repeat = 0;
            }

            // EOB: end of block.
            if sym == eob_symbol {
                break 'outer;
            }

            // Regular symbol: MTF decode.
            let b = mtf.decode((sym - 1) as u8);
            if tt.len() >= max_blocksize as usize {
                return Err(BlockError("data exceeds block size"));
            }
            tt.push(u32::from(b));
            c[b as usize] += 1;
        }

        // Switch Huffman table for next group of 50 symbols.
        sel_idx += 1;
        let sel = *selectors.get(sel_idx)
            .ok_or(BlockError("ran out of selectors"))? as usize;
        current_tree = trees.get(sel)
            .ok_or(BlockError("selector out of range"))?;
    }

    if orig_ptr >= tt.len() {
        return Err(BlockError("orig_ptr out of bounds"));
    }

    // ── Inverse BWT ───────────────────────────────────────────────────────
    let mut t_pos = bwt::inverse_bwt(&mut tt, orig_ptr, c);

    // ── RLE2 decode (bzip2 run-length post-processing) ────────────────────
    // Pre-allocate generously: n bytes for 1-per-iteration + headroom for repeats.
    // Use raw pointer writes to skip Vec::push bounds check on every byte.
    let n = tt.len();
    let out_cap = n + n / 4;
    let mut output = Vec::<u8>::with_capacity(out_cap);
    let mut out_len: usize = 0;
    let mut last_byte: u8 = 0;
    let mut has_last = false;
    let mut byte_repeats: u8 = 0;
    let tt_ptr = tt.as_ptr();

    for _ in 0..n {
        let entry = unsafe { *tt_ptr.add(t_pos as usize) };
        let b = entry as u8;
        t_pos = entry >> 8;

        // Two-step prefetch: read next entry (should be L1 from previous prefetch)
        // and prefetch the entry *after* that, giving L3 two iterations to respond.
        let next_entry = unsafe { *tt_ptr.add(t_pos as usize) };
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                tt_ptr.add((next_entry >> 8) as usize) as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            );
        }

        if byte_repeats == 3 {
            let count = b as usize;
            // Grow if repeat expansion would exceed capacity
            if out_len + count > output.capacity() {
                unsafe { output.set_len(out_len); }
                output.reserve(count);
            }
            unsafe {
                std::ptr::write_bytes(output.as_mut_ptr().add(out_len), last_byte, count);
            }
            out_len += count;
            byte_repeats = 0;
            has_last = false;
            continue;
        }

        if has_last && last_byte == b {
            byte_repeats += 1;
        } else {
            byte_repeats = 0;
        }
        last_byte = b;
        has_last = true;
        // Direct write — capacity guaranteed: out_len ≤ iteration count ≤ n < out_cap
        unsafe { *output.as_mut_ptr().add(out_len) = b; }
        out_len += 1;
    }

    unsafe { output.set_len(out_len); }
    return_tt_buffer(tt);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_known_block() {
        // Use a real bzip2 file: compress "Hello, World!\n" and decode.
        // This is a minimal integration test.
        let compressed = include_bytes!("../test_data/hello.bz2");
        let expected = b"Hello, World!\n";

        // Parse header (4 bytes: "BZh9").
        assert_eq!(&compressed[..3], b"BZh");
        let level = compressed[3] - b'0';
        let max_blocksize = 100_000 * level as u32;

        // Skip header, read block magic.
        let mut reader = BitReader::from_bit_offset(compressed, 4 * 8);
        let magic = reader.read_u64(48).unwrap();
        assert_eq!(magic, crate::BLOCK_MAGIC, "expected block magic");

        let output = decode_block(&mut reader, max_blocksize).unwrap();
        assert_eq!(&output, expected);
    }
}
