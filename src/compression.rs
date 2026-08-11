//! Pluggable compression codecs.
//!
//! The runtime never binds to a single codec. The default codec is Zstandard
//! (zstd, BSD-3-Clause) — widely available and permissively licensed.
//! Compression benefits are estimated before applying a transform, and
//! integrity is verified before and after.

use std::io::Read;

use crate::errors::{ReclaimError, Result};
use crate::integrity::{crc32c, ContentHash};

/// Compression codec interface.
pub trait CompressionCodec: Send + Sync {
    fn name(&self) -> &'static str;
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress while rejecting output larger than `max_output`. Codecs
    /// should override this to enforce the bound while streaming; the default
    /// preserves compatibility for external codecs but can only check after
    /// their `decompress` implementation returns.
    fn decompress_bounded(&self, data: &[u8], max_output: usize) -> Result<Vec<u8>> {
        let output = self.decompress(data)?;
        if output.len() > max_output {
            return Err(ReclaimError::DecompressionFailure(format!(
                "decompressed payload exceeds {max_output} byte limit"
            )));
        }
        Ok(output)
    }
}

/// Zstandard codec with configurable level.
pub struct ZstdCodec {
    level: i32,
}

impl ZstdCodec {
    pub fn new(level: i32) -> ZstdCodec {
        ZstdCodec { level }
    }
}

impl Default for ZstdCodec {
    fn default() -> Self {
        ZstdCodec::new(3)
    }
}

impl CompressionCodec for ZstdCodec {
    fn name(&self) -> &'static str {
        "zstd"
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::stream::encode_all(data, self.level)
            .map_err(|e| ReclaimError::CompressionFailure(format!("zstd compress: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::stream::decode_all(data)
            .map_err(|e| ReclaimError::DecompressionFailure(format!("zstd decompress: {e}")))
    }

    fn decompress_bounded(&self, data: &[u8], max_output: usize) -> Result<Vec<u8>> {
        let decoder = zstd::stream::read::Decoder::new(data)
            .map_err(|e| ReclaimError::DecompressionFailure(format!("zstd decompress: {e}")))?;
        let byte_limit = u64::try_from(max_output)
            .map_err(|_| ReclaimError::InvalidArgument("decompression limit exceeds u64".into()))?
            .saturating_add(1);
        let mut limited = decoder.take(byte_limit);
        let mut output = Vec::with_capacity(max_output.min(64 * 1024));
        limited
            .read_to_end(&mut output)
            .map_err(|e| ReclaimError::DecompressionFailure(format!("zstd decompress: {e}")))?;
        if output.len() > max_output {
            return Err(ReclaimError::DecompressionFailure(format!(
                "zstd decompressed payload exceeds {max_output} byte limit"
            )));
        }
        Ok(output)
    }
}

/// No-op codec.
pub struct NoopCodec;

impl CompressionCodec for NoopCodec {
    fn name(&self) -> &'static str {
        "none"
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress_bounded(&self, data: &[u8], max_output: usize) -> Result<Vec<u8>> {
        if data.len() > max_output {
            return Err(ReclaimError::DecompressionFailure(format!(
                "uncompressed payload exceeds {max_output} byte limit"
            )));
        }
        Ok(data.to_vec())
    }
}

/// Compression outcome with integrity results.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompressionResult {
    pub codec: String,
    pub original_size: u64,
    pub compressed_size: u64,
    pub ratio: f64,
    pub original_hash: ContentHash,
    pub compressed_hash: ContentHash,
    pub original_crc32c: u32,
    pub compressed_crc32c: u32,
}

/// Compress a payload with integrity verification before and after.
/// Returns an error if the round-trip does not reproduce the original.
pub fn compress_verified(codec: &dyn CompressionCodec, data: &[u8]) -> Result<CompressionResult> {
    compress_verified_with_bytes(codec, data).map(|(_, result)| result)
}

/// Compress a payload with integrity verification, returning both the
/// compressed bytes and the accounting result.
pub fn compress_verified_with_bytes(
    codec: &dyn CompressionCodec,
    data: &[u8],
) -> Result<(Vec<u8>, CompressionResult)> {
    let original_hash = ContentHash::of(data);
    let original_crc = crc32c(data);
    let compressed = codec.compress(data)?;
    let compressed_hash = ContentHash::of(&compressed);
    let compressed_crc = crc32c(&compressed);
    // Round-trip check: compressed state must decompress back to the
    // original payload before we report success.
    // The correct output size is known. Enforce it during decoding so a
    // broken/corrupt codec cannot turn the round-trip check into an
    // unbounded expansion.
    let roundtrip = codec.decompress_bounded(&compressed, data.len())?;
    if roundtrip != data {
        return Err(ReclaimError::CompressionFailure(
            "compression round-trip produced different payload".into(),
        ));
    }
    let result = CompressionResult {
        codec: codec.name().to_string(),
        original_size: data.len() as u64,
        compressed_size: compressed.len() as u64,
        ratio: if data.is_empty() {
            1.0
        } else {
            compressed.len() as f64 / data.len() as f64
        },
        original_hash,
        compressed_hash,
        original_crc32c: original_crc,
        compressed_crc32c: compressed_crc,
    };
    Ok((compressed, result))
}

/// Estimate compression benefit (returns None when the payload is likely
/// incompressible or too small to bother with).
pub fn benefit_estimate(codec: &dyn CompressionCodec, data: &[u8]) -> Result<Option<f64>> {
    if data.len() < 64 {
        return Ok(None);
    }
    let sample = &data[..data.len().min(4096)];
    let compressed = codec.compress(sample)?;
    let ratio = compressed.len() as f64 / sample.len() as f64;
    Ok((ratio < 0.95).then_some(ratio))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_roundtrip() {
        let codec = ZstdCodec::new(5);
        let data: Vec<u8> = (0..10_000u32)
            .flat_map(|i| format!("{i:08}").into_bytes())
            .collect();
        let result = compress_verified(&codec, &data).unwrap();
        assert_eq!(result.original_hash, ContentHash::of(&data));
        assert_eq!(result.original_size, data.len() as u64);
        assert!(result.compressed_size > 0);
        assert!(result.ratio < 1.0);
    }

    #[test]
    fn noop_roundtrip() {
        let codec = NoopCodec;
        let data = b"hello world".to_vec();
        let result = compress_verified(&codec, &data).unwrap();
        assert_eq!(result.compressed_size, result.original_size);
        assert_eq!(result.ratio, 1.0);
    }

    #[test]
    fn decompression_of_garbage_fails() {
        let codec = ZstdCodec::new(3);
        assert!(codec.decompress(b"not zstd data").is_err());
    }

    #[test]
    fn benefit_estimate_small_payloads_skip() {
        let codec = ZstdCodec::new(3);
        assert!(benefit_estimate(&codec, b"tiny").unwrap().is_none());
    }

    #[test]
    fn bounded_zstd_decompression_rejects_expansion() {
        let codec = ZstdCodec::new(3);
        let compressed = codec.compress(&vec![0u8; 1024 * 1024]).unwrap();
        let error = codec.decompress_bounded(&compressed, 1024).unwrap_err();
        assert!(matches!(error, ReclaimError::DecompressionFailure(_)));
    }
}
