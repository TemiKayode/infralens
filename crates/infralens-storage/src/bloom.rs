//! Simple counting bloom filter using double-hashing (SipHash seeds).
//!
//! Serialised format:
//!   [num_bits: u64 LE] [num_hashes: u32 LE] [bits: ceil(num_bits/8) bytes]

use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits:       Vec<u8>, // bit-array, each bit is one cell
    num_bits:   u64,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new filter sized for `expected_items` at `fp_rate` false-positive probability.
    pub fn new(expected_items: u64, fp_rate: f64) -> Self {
        // m = -n * ln(p) / (ln(2)^2)
        let m = (-(expected_items as f64) * fp_rate.ln() / (2f64.ln().powi(2))).ceil() as u64;
        let num_bits   = m.max(64);
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.max(1).min(16);
        let byte_len   = ((num_bits + 7) / 8) as usize;

        Self { bits: vec![0u8; byte_len], num_bits, num_hashes }
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let bit = self.hash_index(key, i);
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// Test if a key is probably in the set.
    pub fn contains(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let bit = self.hash_index(key, i);
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash_index(&self, key: &[u8], seed: u32) -> u64 {
        // Double-hashing: h(i) = h1 + i*h2 (mod m)
        let h1 = sip_hash(key, 0x_cafe_f00d_dead_beef);
        let h2 = sip_hash(key, seed as u64 ^ 0x_0102_0304_0506_0708);
        (h1.wrapping_add((seed as u64).wrapping_mul(h2))) % self.num_bits
    }

    /// Serialise to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&self.num_bits.to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialise from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 12 { return None; }
        let num_bits   = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let num_hashes = u32::from_le_bytes(data[8..12].try_into().ok()?);
        let byte_len   = ((num_bits + 7) / 8) as usize;
        if data.len() < 12 + byte_len { return None; }
        Some(Self {
            bits:       data[12..12 + byte_len].to_vec(),
            num_bits,
            num_hashes,
        })
    }
}

/// Minimal SipHash-1-3 implementation (simplified for correctness, not speed).
fn sip_hash(data: &[u8], seed: u64) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut h);
    data.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let keys: Vec<String> = (0..500).map(|i| format!("key-{i}")).collect();
        for k in &keys { bf.insert(k.as_bytes()); }
        for k in &keys {
            assert!(bf.contains(k.as_bytes()), "false negative for {k}");
        }
    }

    #[test]
    fn reasonable_fp_rate() {
        let mut bf = BloomFilter::new(1000, 0.05);
        for i in 0..1000u64 { bf.insert(&i.to_le_bytes()); }
        let mut fp = 0u32;
        for i in 1000..2000u64 {
            if bf.contains(&i.to_le_bytes()) { fp += 1; }
        }
        // Allow 2x the target FP rate as tolerance for the simple hash impl.
        assert!((fp as f64 / 1000.0) < 0.10, "FP rate too high: {fp}/1000");
    }

    #[test]
    fn serialise_roundtrip() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");
        let bytes = bf.to_bytes();
        let bf2   = BloomFilter::from_bytes(&bytes).unwrap();
        assert!(bf2.contains(b"hello"));
        assert!(bf2.contains(b"world"));
        assert!(!bf2.contains(b"nothere_very_unique_xyzzy"));
    }
}
