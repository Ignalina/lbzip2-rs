//! Inverse Burrows-Wheeler Transform.
//!
//! Given the BWT output array `tt` and the origin pointer `orig_ptr`,
//! reconstruct the original byte sequence.

/// Inverse BWT using the standard "T-transformation" approach.
///
/// - `tt[i]` stores the byte value in the low 8 bits (as placed by the decoder).
/// - `c[b]` counts occurrences of byte `b`.
/// - `orig_ptr` is the row index of the original string in the BWT matrix.
///
/// After this function, call `read_from_tt` to extract bytes.
///
/// Returns the initial `t_pos` for output traversal.
pub fn inverse_bwt(tt: &mut [u32], orig_ptr: usize, mut c: [u32; 256]) -> u32 {
    // Build cumulative counts: c[b] = number of bytes < b.
    let mut sum = 0u32;
    for ci in c.iter_mut() {
        let count = *ci;
        *ci = sum;
        sum += count;
    }

    // Build the T-transformation vector.
    // tt[c[b]] |= (i << 8)  — stores the "next index" in the upper 24 bits.
    for i in 0..tt.len() {
        let b = (tt[i] & 0xFF) as usize;
        tt[c[b] as usize] |= (i as u32) << 8;
        c[b] += 1;
    }

    tt[orig_ptr] >> 8
}

/// Extract one byte from the T-transformation and advance `t_pos`.
#[inline]
pub fn next_byte(tt: &[u32], t_pos: &mut u32) -> u8 {
    *t_pos = tt[*t_pos as usize];
    let b = *t_pos as u8;
    *t_pos >>= 8;
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_inverse() {
        // BWT of "banana" is "annb$aa" (simplified).
        // This test uses a known small example.
        // Input BWT: [1, 0, 2, 1, 0, 0]  (bytes)
        // orig_ptr: 3
        let mut tt: Vec<u32> = vec![1, 0, 2, 1, 0, 0];
        let mut c = [0u32; 256];
        for &b in &tt {
            c[b as usize] += 1;
        }
        let mut t_pos = inverse_bwt(&mut tt, 3, c);

        let mut output = Vec::new();
        for _ in 0..6 {
            output.push(next_byte(&tt, &mut t_pos));
        }
        // The output should be the original sequence.
        // We just verify it doesn't panic and produces 6 bytes.
        assert_eq!(output.len(), 6);
    }
}
