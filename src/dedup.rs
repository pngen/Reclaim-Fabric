//! Content-aware deduplication.
//!
//! Multiple logical objects may reference one physical payload via a
//! cryptographic content identity (SHA-256). Reference ownership is explicit
//! and durable: a logical object's reclamation never destroys physical
//! content still referenced by another live object.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;

/// One deduplicated physical payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    /// Content identity (SHA-256 of payload).
    pub content_hash: ContentHash,
    /// Backend + key where the single physical copy lives.
    pub backend: String,
    pub key: String,
    /// Number of logical objects referencing this payload.
    pub ref_count: u64,
    pub payload_size: u64,
}

/// In-memory dedup registry (authoritative copy lives in the store; this is a
/// working cache/validator).
#[derive(Debug, Default)]
pub struct DedupRegistry {
    by_hash: HashMap<(ContentHash, String), DedupEntry>,
    by_key: HashMap<(String, String), ContentHash>,
}

impl DedupRegistry {
    pub fn new() -> DedupRegistry {
        DedupRegistry::default()
    }

    pub fn insert(&mut self, entry: DedupEntry) -> Result<()> {
        if entry.backend.is_empty() || entry.key.is_empty() || entry.ref_count == 0 {
            return Err(ReclaimError::Dedup(
                "dedup entry requires backend, key, and at least one reference".into(),
            ));
        }
        let identity = (entry.content_hash, entry.backend.clone());
        let physical = (entry.backend.clone(), entry.key.clone());
        if let Some(existing_hash) = self.by_key.get(&physical) {
            if *existing_hash != entry.content_hash {
                return Err(ReclaimError::Dedup(format!(
                    "physical key {} on {} is already assigned to different content",
                    entry.key, entry.backend
                )));
            }
        }
        if let Some(previous) = self.by_hash.insert(identity, entry.clone()) {
            self.by_key.remove(&(previous.backend, previous.key));
        }
        self.by_key.insert(physical, entry.content_hash);
        Ok(())
    }

    /// Deterministic legacy lookup across backends. Prefer `get_on` whenever
    /// backend identity is available.
    pub fn get(&self, hash: &ContentHash) -> Option<&DedupEntry> {
        self.by_hash
            .values()
            .filter(|entry| entry.content_hash == *hash)
            .min_by(|a, b| a.backend.cmp(&b.backend).then_with(|| a.key.cmp(&b.key)))
    }

    pub fn get_on(&self, hash: &ContentHash, backend: &str) -> Option<&DedupEntry> {
        self.by_hash.get(&(*hash, backend.to_string()))
    }

    pub fn remove(&mut self, hash: &ContentHash) {
        let identities: Vec<_> = self
            .by_hash
            .keys()
            .filter(|(content_hash, _)| content_hash == hash)
            .cloned()
            .collect();
        for identity in identities {
            if let Some(entry) = self.by_hash.remove(&identity) {
                self.by_key.remove(&(entry.backend, entry.key));
            }
        }
    }

    pub fn remove_on(&mut self, hash: &ContentHash, backend: &str) {
        if let Some(entry) = self.by_hash.remove(&(*hash, backend.to_string())) {
            self.by_key.remove(&(entry.backend, entry.key));
        }
    }

    fn unique_backend(&self, hash: &ContentHash) -> Result<String> {
        let mut backends = self
            .by_hash
            .keys()
            .filter(|(content_hash, _)| content_hash == hash)
            .map(|(_, backend)| backend.as_str());
        let backend = backends
            .next()
            .ok_or_else(|| ReclaimError::Dedup(format!("unknown content {hash}")))?;
        if backends.next().is_some() {
            return Err(ReclaimError::Dedup(format!(
                "content {hash} exists on multiple backends; backend is required"
            )));
        }
        Ok(backend.to_string())
    }

    /// Increment the reference count for a content hash.
    pub fn acquire(&mut self, hash: &ContentHash) -> Result<()> {
        let backend = self.unique_backend(hash)?;
        self.acquire_on(hash, &backend)
    }

    pub fn acquire_on(&mut self, hash: &ContentHash, backend: &str) -> Result<()> {
        let e = self
            .by_hash
            .get_mut(&(*hash, backend.to_string()))
            .ok_or_else(|| {
                ReclaimError::Dedup(format!(
                    "cannot acquire unknown content {hash} on {backend}"
                ))
            })?;
        e.ref_count = e
            .ref_count
            .checked_add(1)
            .ok_or_else(|| ReclaimError::Dedup("dedup ref_count overflow".into()))?;
        Ok(())
    }

    /// Decrement the reference count. Returns true when the payload becomes
    /// unreferenced and the caller may physically delete it.
    pub fn release(&mut self, hash: &ContentHash) -> Result<bool> {
        let backend = self.unique_backend(hash)?;
        self.release_on(hash, &backend)
    }

    pub fn release_on(&mut self, hash: &ContentHash, backend: &str) -> Result<bool> {
        let e = self
            .by_hash
            .get_mut(&(*hash, backend.to_string()))
            .ok_or_else(|| {
                ReclaimError::Dedup(format!(
                    "cannot release unknown content {hash} on {backend}"
                ))
            })?;
        if e.ref_count == 0 {
            return Err(ReclaimError::Dedup(format!(
                "release on content {hash} with zero references"
            )));
        }
        e.ref_count -= 1;
        Ok(e.ref_count == 0)
    }

    /// Collision-safe: verify that the physical payload actually hashes to the
    /// content identity before trusting a dedup hit.
    pub fn verify(&self, hash: &ContentHash, payload: &[u8]) -> Result<()> {
        crate::integrity::verify_sha256(payload, hash)
            .map_err(|e| ReclaimError::Dedup(format!("dedup collision check failed: {e}")))
    }

    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty()
    }

    pub fn all(&self) -> Vec<DedupEntry> {
        let mut v: Vec<DedupEntry> = self.by_hash.values().cloned().collect();
        v.sort_by(|a, b| {
            a.content_hash
                .cmp(&b.content_hash)
                .then_with(|| a.backend.cmp(&b.backend))
                .then_with(|| a.key.cmp(&b.key))
        });
        v
    }
}

/// Find the canonical key for a payload among live logical objects. Returns
/// the key + backend if an existing deduplicated copy can be reused.
pub fn find_canonical<'a>(
    entries: impl IntoIterator<Item = &'a DedupEntry>,
    hash: &ContentHash,
) -> Option<(String, String)> {
    entries
        .into_iter()
        .filter(|e| e.content_hash == *hash && e.ref_count > 0)
        .min_by(|a, b| a.backend.cmp(&b.backend).then_with(|| a.key.cmp(&b.key)))
        .map(|e| (e.backend.clone(), e.key.clone()))
}

/// Compute a dedup storage key from a content hash.
pub fn dedup_key(hash: &ContentHash) -> String {
    format!("dedup-{}", hash)
}

/// Validate ref-count bookkeeping after restart: total refs must equal the
/// number of live logical references.
pub fn validate_ref_counts(
    entries: &[DedupEntry],
    live_refs: impl Fn(&ContentHash) -> u64,
) -> Result<()> {
    let mut totals: std::collections::BTreeMap<ContentHash, u64> =
        std::collections::BTreeMap::new();
    for e in entries {
        let total = totals.entry(e.content_hash).or_default();
        *total = total
            .checked_add(e.ref_count)
            .ok_or_else(|| ReclaimError::Recovery("dedup ref-count overflow".into()))?;
    }
    for (hash, stored) in totals {
        let expected = live_refs(&hash);
        if stored != expected {
            return Err(ReclaimError::Recovery(format!(
                "dedup ref-count mismatch for {}: stored {}, expected {expected}",
                hash, stored
            )));
        }
    }
    Ok(())
}

/// Marker type for objects whose payload is deduplicated (used in state
/// transitions to/from DEDUPED).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DedupKind {
    /// Payload owned exclusively (not deduplicated).
    Exclusive,
    /// Payload shared with other logical objects.
    Shared,
}

/// Helper for tests: a dummy payload id derived from a Uuid.
pub fn dummy_payload(id: &Uuid) -> Vec<u8> {
    id.as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_counting() {
        let mut reg = DedupRegistry::new();
        let hash = ContentHash::of(b"shared payload");
        reg.insert(DedupEntry {
            content_hash: hash,
            backend: "mem".into(),
            key: "dedup-1".into(),
            ref_count: 1,
            payload_size: 14,
        })
        .unwrap();
        reg.acquire(&hash).unwrap();
        reg.acquire(&hash).unwrap();
        assert!(!reg.release(&hash).unwrap());
        assert!(!reg.release(&hash).unwrap());
        assert!(reg.release(&hash).unwrap()); // last release -> delete ok
                                              // Double release on zero refs fails.
        assert!(reg.release(&hash).is_err());
    }

    #[test]
    fn unknown_hash_rejected() {
        let mut reg = DedupRegistry::new();
        assert!(reg.acquire(&ContentHash::of(b"x")).is_err());
        assert!(reg.release(&ContentHash::of(b"x")).is_err());
    }

    #[test]
    fn collision_safe_verification() {
        let mut reg = DedupRegistry::new();
        let hash = ContentHash::of(b"real");
        reg.insert(DedupEntry {
            content_hash: hash,
            backend: "mem".into(),
            key: "k".into(),
            ref_count: 1,
            payload_size: 4,
        })
        .unwrap();
        reg.verify(&hash, b"real").unwrap();
        assert!(reg.verify(&hash, b"fake").is_err());
    }

    #[test]
    fn canonical_lookup() {
        let mut reg = DedupRegistry::new();
        let hash = ContentHash::of(b"p");
        reg.insert(DedupEntry {
            content_hash: hash,
            backend: "mem".into(),
            key: "canonical".into(),
            ref_count: 1,
            payload_size: 1,
        })
        .unwrap();
        let (backend, key) = find_canonical(reg.all().iter(), &hash).unwrap();
        assert_eq!(backend, "mem");
        assert_eq!(key, "canonical");
        // Exhausted entry is not canonical.
        reg.release(&hash).unwrap();
        assert!(find_canonical(reg.all().iter(), &hash).is_none());
    }

    #[test]
    fn same_content_on_multiple_backends_is_not_overwritten() {
        let mut reg = DedupRegistry::new();
        let hash = ContentHash::of(b"shared");
        for (backend, key) in [("z-backend", "z-key"), ("a-backend", "a-key")] {
            reg.insert(DedupEntry {
                content_hash: hash,
                backend: backend.into(),
                key: key.into(),
                ref_count: 1,
                payload_size: 6,
            })
            .unwrap();
        }
        assert_eq!(reg.len(), 2);
        assert!(reg.acquire(&hash).is_err(), "ambiguous legacy acquire");
        reg.acquire_on(&hash, "a-backend").unwrap();
        assert_eq!(reg.get_on(&hash, "a-backend").unwrap().ref_count, 2);
        assert_eq!(reg.get_on(&hash, "z-backend").unwrap().ref_count, 1);

        let entries = reg.all();
        assert_eq!(
            find_canonical(entries.iter(), &hash).unwrap().0,
            "a-backend"
        );
        validate_ref_counts(&entries, |_| 3).unwrap();
    }
}
