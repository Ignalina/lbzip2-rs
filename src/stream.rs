//! bzip2 stream decoder — sequential multi-block decompression.
//!
//! Decodes a complete bzip2 stream (header + N blocks + EOS marker).
//! This is the single-threaded reference path; the parallel pipeline
//! will split blocks across workers instead.

use crate::bitreader::BitReader;
use crate::block;
use crate::BLOCK_MAGIC;
use crate::FINAL_MAGIC;

/// Decompress a complete bzip2 stream from `data`.
/// Returns the fully decompressed output.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, block::BlockError> {
    if data.len() < 4 {
        return Err(block::BlockError("input too short for bzip2 header"));
    }
    if &data[..2] != b"BZ" {
        return Err(block::BlockError("bad bzip2 signature"));
    }
    if data[2] != b'h' {
        return Err(block::BlockError("only huffman bzip2 supported"));
    }
    let level = data[3];
    if !(b'1'..=b'9').contains(&level) {
        return Err(block::BlockError("invalid bzip2 block size level"));
    }
    let max_blocksize = 100_000 * (level - b'0') as u32;

    let mut reader = BitReader::from_bit_offset(data, 4 * 8); // skip "BZhN"
    let mut output = Vec::new();

    loop {
        let magic = reader.read_u64(48)
            .ok_or(block::BlockError("unexpected end of stream"))?;

        if magic == BLOCK_MAGIC {
            let block_data = block::decode_block(&mut reader, max_blocksize)?;
            output.extend_from_slice(&block_data);
        } else if magic == FINAL_MAGIC {
            // End of stream — skip CRC32 (32 bits) and optional padding
            break;
        } else {
            return Err(block::BlockError("invalid block magic"));
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompress_hello() {
        let compressed = include_bytes!("../test_data/hello.bz2");
        let output = decompress(compressed).unwrap();
        assert_eq!(&output, b"Hello, World!\n");
    }

    #[test]
    fn decompress_liechtenstein() {
        let compressed = include_bytes!("../test_data/liechtenstein.osm.bz2");
        let output = decompress(compressed).unwrap();
        // Verify non-empty and starts with XML header
        assert!(output.len() > 1_000_000, "expected multi-MB output");
        let header = std::str::from_utf8(&output[..100]).unwrap();
        assert!(header.contains("<?xml"), "expected XML header, got: {}", &header[..60]);
    }
}
