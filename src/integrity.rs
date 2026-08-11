//! Integrity primitives: SHA-256 durable content identity and CRC-32C fast
//! corruption detection.
//!
//! SHA-256 is the canonical content identity for deduplication and archival.
//! CRC-32C is available for fast transport/compression corruption checks
//! where a full hash would be too expensive. Archives use SHA-256.

use std::fmt;

use sha2::{Digest, Sha256};

use crate::errors::{ReclaimError, Result};

/// 32-byte SHA-256 content hash.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub fn of(data: &[u8]) -> ContentHash {
        let mut h = Sha256::new();
        h.update(data);
        ContentHash(h.finalize().into())
    }

    /// Hash of an empty payload (used to reject empty objects explicitly).
    pub fn empty() -> ContentHash {
        ContentHash::of(&[])
    }

    pub fn from_hex(s: &str) -> Result<ContentHash> {
        let mut out = [0u8; 32];
        if s.len() != 64 {
            return Err(ReclaimError::InvalidArgument(
                "content hash must be 64 hex chars".into(),
            ));
        }
        fn nibble(byte: u8) -> Option<u8> {
            match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            }
        }
        for (i, pair) in s.as_bytes().chunks_exact(2).enumerate() {
            let high = nibble(pair[0])
                .ok_or_else(|| ReclaimError::InvalidArgument("invalid content hash hex".into()))?;
            let low = nibble(pair[1])
                .ok_or_else(|| ReclaimError::InvalidArgument("invalid content hash hex".into()))?;
            out[i] = (high << 4) | low;
        }
        Ok(ContentHash(out))
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for ContentHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// CRC-32C (Castagnoli) checksum of a payload.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Incremental verifier for streaming hashing.
pub struct StreamingHasher {
    inner: Sha256,
}

impl StreamingHasher {
    pub fn new() -> StreamingHasher {
        StreamingHasher {
            inner: Sha256::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    pub fn finish(self) -> ContentHash {
        ContentHash(self.inner.finalize().into())
    }
}

impl Default for StreamingHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Verify that `data` hashes to `expected`. Returns an integrity error on
/// mismatch so callers can fail closed.
pub fn verify_sha256(data: &[u8], expected: &ContentHash) -> Result<()> {
    let actual = ContentHash::of(data);
    if &actual != expected {
        return Err(ReclaimError::IntegrityFailure(format!(
            "sha256 mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_deterministic() {
        let a = ContentHash::of(b"hello");
        let b = ContentHash::of(b"hello");
        let c = ContentHash::of(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(format!("{a}"), format!("{a}"));
    }

    #[test]
    fn sha256_known_vector() {
        // SHA-256 of "abc"
        assert_eq!(
            ContentHash::of(b"abc").to_string(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn verify_mismatch_fails() {
        assert!(verify_sha256(b"a", &ContentHash::of(b"b")).is_err());
        assert!(verify_sha256(b"a", &ContentHash::of(b"a")).is_ok());
    }

    #[test]
    fn hex_roundtrip() {
        let h = ContentHash::of(b"payload");
        assert_eq!(ContentHash::from_hex(&h.to_string()).unwrap(), h);
        assert!(ContentHash::from_hex("abc").is_err());
        // A 64-byte UTF-8 string used to panic while slicing at a non-char
        // boundary. Malformed external input must be rejected, never unwind.
        let non_ascii = format!("{}x", "€".repeat(21));
        assert_eq!(non_ascii.len(), 64);
        assert!(ContentHash::from_hex(&non_ascii).is_err());
    }
}
