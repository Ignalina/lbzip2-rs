//! Canonical Huffman tree decoder for bzip2.
//!
//! bzip2 uses up to 6 Huffman tables per block, switching every 50 symbols.
//! Each table encodes up to 258 symbols (256 bytes + RUNA + RUNB + EOB).
//!
//! Uses a flat lookup table for codes ≤ FAST_BITS with tree fallback for longer codes.

/// Max bits for the fast lookup table.  12 covers the vast majority of bzip2 codes.
const FAST_BITS: u8 = 12;

/// A Huffman decoding tree with fast table lookup.
pub struct HuffmanTree {
    /// Fast lookup: indexed by the top FAST_BITS of the bitstream.
    /// Each entry: (symbol, bit_length).  If bit_length == 0 → need tree fallback.
    fast_table: Vec<(u16, u8)>,
    /// Slow path: flat node array.  Each node stores [left_child, right_child].
    /// Positive values = index of child node.
    /// Negative values = -(symbol + 1), i.e. a leaf.
    nodes: Vec<[i32; 2]>,
    /// Minimum code length (for fast-path skip).
    min_len: u8,
}

impl HuffmanTree {
    /// Build a Huffman tree from code lengths.
    ///
    /// `lengths[i]` is the bit-length of symbol `i`.  Lengths of 0 mean the
    /// symbol is unused.  bzip2 lengths are in range 1..=20.
    pub fn from_lengths(lengths: &[u8]) -> Result<Self, &'static str> {
        // Assign canonical codes: sort by (length, symbol), assign incrementally.
        let mut symbols: Vec<(u16, u8)> = lengths.iter()
            .enumerate()
            .filter(|(_, len)| **len > 0)
            .map(|(sym, len)| (sym as u16, *len))
            .collect();

        if symbols.is_empty() {
            return Err("no symbols");
        }

        symbols.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));

        let min_len = symbols[0].1;

        // Build tree by inserting each symbol.
        let mut nodes: Vec<[i32; 2]> = vec![[0, 0]]; // root at index 0

        let mut code: u32 = 0;
        let mut prev_len: u8 = symbols[0].1;

        // Also collect (code, length, symbol) for fast table construction
        let mut code_entries: Vec<(u32, u8, u16)> = Vec::with_capacity(symbols.len());

        for &(sym, len) in &symbols {
            code <<= len - prev_len;
            prev_len = len;

            code_entries.push((code, len, sym));

            // Walk the tree for `len` bits of `code`, creating nodes as needed.
            let mut node_idx: usize = 0;
            for bit_pos in (0..len).rev() {
                let bit = ((code >> bit_pos) & 1) as usize;
                let child = nodes[node_idx][bit];
                if child > 0 {
                    node_idx = child as usize;
                } else if bit_pos > 0 {
                    // Create internal node.
                    let new_idx = nodes.len();
                    nodes.push([0, 0]);
                    nodes[node_idx][bit] = new_idx as i32;
                    node_idx = new_idx;
                } else {
                    // Leaf.
                    nodes[node_idx][bit] = -(sym as i32 + 1);
                }
            }

            code += 1;
        }

        // Build fast lookup table
        let table_size = 1usize << FAST_BITS;
        let mut fast_table = vec![(0u16, 0u8); table_size];

        for &(c, len, sym) in &code_entries {
            if len <= FAST_BITS {
                // This code maps to multiple table entries (padded with all suffix combinations)
                let pad = FAST_BITS - len;
                let base = (c as usize) << pad;
                for suffix in 0..(1usize << pad) {
                    fast_table[base | suffix] = (sym, len);
                }
            }
        }

        Ok(Self { fast_table, nodes, min_len })
    }

    /// Decode one symbol using fast table lookup with tree fallback.
    #[inline(always)]
    pub fn decode(&self, reader: &mut super::bitreader::BitReader<'_>) -> Option<u16> {
        // Try peek FAST_BITS from the stream
        if let Some(bits) = reader.peek(FAST_BITS) {
            let entry = unsafe { self.fast_table.get_unchecked(bits as usize) };
            if entry.1 > 0 {
                // Fast path: symbol found in table
                reader.consume(entry.1);
                return Some(entry.0);
            }
        }

        // Slow path: walk the tree bit by bit
        self.decode_slow(reader)
    }

    /// Slow decode path: walk the tree node by node.
    #[cold]
    fn decode_slow(&self, reader: &mut super::bitreader::BitReader<'_>) -> Option<u16> {
        let mut node_idx: usize = 0;
        loop {
            let bit = reader.read_bit()? as usize;
            let child = self.nodes[node_idx][bit];
            if child < 0 {
                return Some((-child - 1) as u16);
            }
            node_idx = child as usize;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitreader::BitReader;

    #[test]
    fn simple_tree() {
        // Symbol 0: length 1 (code: 0)
        // Symbol 1: length 2 (code: 10)
        // Symbol 2: length 2 (code: 11)
        let tree = HuffmanTree::from_lengths(&[1, 2, 2]).unwrap();
        // Bits: 0 10 11 0
        let data = [0b0_10_11_0_00];
        let mut r = BitReader::new(&data);
        assert_eq!(tree.decode(&mut r), Some(0));
        assert_eq!(tree.decode(&mut r), Some(1));
        assert_eq!(tree.decode(&mut r), Some(2));
        assert_eq!(tree.decode(&mut r), Some(0));
    }

    #[test]
    fn fast_table_coverage() {
        // Test with codes of various lengths up to FAST_BITS
        // 4 symbols: lengths [2, 2, 3, 3]
        // Canonical: 00, 01, 100, 101
        let tree = HuffmanTree::from_lengths(&[2, 2, 3, 3]).unwrap();
        // Encode: sym0(00) sym1(01) sym2(100) sym3(101)
        // Bits: 00_01_100_101_0000 (padded)
        let data = [0b00_01_100_1, 0b01_000000];
        let mut r = BitReader::new(&data);
        assert_eq!(tree.decode(&mut r), Some(0));
        assert_eq!(tree.decode(&mut r), Some(1));
        assert_eq!(tree.decode(&mut r), Some(2));
        assert_eq!(tree.decode(&mut r), Some(3));
    }
}
