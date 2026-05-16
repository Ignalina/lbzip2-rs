//! Move-to-front transform decoder.
//!
//! bzip2 applies MTF encoding after BWT to cluster repeated symbols near
//! index 0, making Huffman coding more effective.

pub struct MtfDecoder {
    symbols: [u8; 256],
}

impl MtfDecoder {
    /// Standard MTF decoder: symbols[i] = i.
    pub fn new() -> Self {
        let mut symbols = [0u8; 256];
        for (i, s) in symbols.iter_mut().enumerate() {
            *s = i as u8;
        }
        Self { symbols }
    }

    /// MTF decoder with a custom initial symbol order.
    pub fn with_symbols(symbols: [u8; 256]) -> Self {
        Self { symbols }
    }

    /// Decode: return the symbol at position `n`, then move it to front.
    #[inline(always)]
    pub fn decode(&mut self, n: u8) -> u8 {
        let idx = n as usize;
        let b = self.symbols[idx];
        if idx == 0 {
            return b;
        }
        if idx == 1 {
            self.symbols[1] = self.symbols[0];
            self.symbols[0] = b;
            return b;
        }
        // Shift symbols[0..idx] right by one, place b at front.
        self.symbols.copy_within(..idx, 1);
        self.symbols[0] = b;
        b
    }

    /// Return the symbol currently at front (position 0).
    #[inline]
    pub fn first(&self) -> u8 {
        self.symbols[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_mtf() {
        let mut mtf = MtfDecoder::new();
        assert_eq!(mtf.decode(5), 5);
        assert_eq!(mtf.first(), 5);
        // After decoding 5: symbols = [5, 0, 1, 2, 3, 4, 6, 7, ...]
        assert_eq!(mtf.decode(0), 5); // 5 is at front
        assert_eq!(mtf.decode(1), 0); // 0 is at position 1
    }
}
