//! Physical payload backends.
//!
//! A `Backend` stores opaque byte payloads keyed by string. Reclaim Fabric
//! tracks *where* bytes live through backends but never depends on a single
//! storage technology. Backends are the boundary where GPU/accelerator memory
//! could be plugged in later behind the same trait (feature-gated, never
//! required for core correctness).

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;

/// Payload backend interface.
pub trait Backend: Send + Sync {
    /// Stable identifier of this backend ("memory", "file:/x/y").
    fn id(&self) -> &str;

    /// Store `data` at `key`, overwriting. Returns bytes stored.
    fn put(&self, key: &str, data: &[u8]) -> Result<u64>;

    /// Read payload at `key`.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Delete payload at `key`. Idempotent: missing key is Ok.
    fn delete(&self, key: &str) -> Result<()>;

    /// Check whether a payload exists at `key`. I/O and backend failures must
    /// be surfaced because recovery treats this result as physical truth.
    fn exists(&self, key: &str) -> Result<bool>;

    /// Verify payload integrity at `key` against expected content hash.
    fn verify(&self, key: &str, expected: &ContentHash) -> Result<()>;

    /// Current total bytes stored (for stats/pressure reporting).
    fn total_bytes(&self) -> u64;

    /// List keys currently stored.
    fn keys(&self) -> Vec<String>;

    /// Human-readable backend kind.
    fn kind(&self) -> &'static str;
}

/// In-memory payload backend (host memory). Contents do not survive restart.
pub struct MemoryBackend {
    id: String,
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new(id: impl Into<String>) -> MemoryBackend {
        MemoryBackend {
            id: id.into(),
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl Backend for MemoryBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<u64> {
        self.data
            .lock()
            .map_err(|_| ReclaimError::Backend("memory backend lock poisoned".into()))?
            .insert(key.to_string(), data.to_vec());
        Ok(data.len() as u64)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.data
            .lock()
            .map_err(|_| ReclaimError::Backend("memory backend lock poisoned".into()))?
            .get(key)
            .cloned()
            .ok_or_else(|| ReclaimError::NotFound(format!("payload {key}")))
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.data
            .lock()
            .map_err(|_| ReclaimError::Backend("memory backend lock poisoned".into()))?
            .remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self
            .data
            .lock()
            .map_err(|_| ReclaimError::Backend("memory backend lock poisoned".into()))?
            .contains_key(key))
    }

    fn verify(&self, key: &str, expected: &ContentHash) -> Result<()> {
        let data = self.get(key)?;
        crate::integrity::verify_sha256(&data, expected)
    }

    fn total_bytes(&self) -> u64 {
        self.data
            .lock()
            .map(|g| {
                g.values().fold(0u64, |total, value| {
                    total.saturating_add(value.len() as u64)
                })
            })
            .unwrap_or(0)
    }

    fn keys(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .data
            .lock()
            .map(|g| g.keys().cloned().collect())
            .unwrap_or_default();
        v.sort();
        v
    }

    fn kind(&self) -> &'static str {
        "memory"
    }
}

/// Durable local-filesystem payload backend.
///
/// Writes are atomic: write to a temp file in the same directory, fsync, then
/// rename. Keys are validated against path traversal and platform aliases.
/// Normal error paths remove their owned temp file. A process crash can leave
/// a `.tmp-` file; those files are excluded from payload listing/accounting
/// and are not deleted automatically because another process may be using the
/// same backend root. Parent directory entries are fsynced on Unix after
/// publication and deletion; Rust's standard Windows filesystem API does not
/// expose an equivalent directory-fsync step.
pub struct FileBackend {
    id: String,
    root: PathBuf,
}

impl FileBackend {
    pub fn new(id: impl Into<String>, root: impl AsRef<Path>) -> Result<FileBackend> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| {
            ReclaimError::Io(format!("creating backend dir {}: {e}", root.display()))
        })?;
        // Keep later operations independent of process-wide current-directory
        // changes and normalize any symlink/`..` components once at open.
        let root = fs::canonicalize(&root).map_err(|e| {
            ReclaimError::Io(format!("resolving backend dir {}: {e}", root.display()))
        })?;
        Ok(FileBackend {
            id: id.into(),
            root,
        })
    }

    /// Validate a portable single-component key. Temporary-file namespace and
    /// Windows alternate-stream/device spellings are reserved even on Unix so
    /// a key has the same meaning on every supported host.
    pub fn validate_key(key: &str) -> Result<()> {
        // Leave room for the temp prefix and UUID on filesystems with a common
        // 255-byte component limit.
        if key.is_empty() || key.len() > 200 {
            return Err(ReclaimError::InvalidArgument(
                "invalid payload key length".into(),
            ));
        }
        if key == "." || key == ".." || key.starts_with(".tmp-") {
            return Err(ReclaimError::InvalidArgument("reserved payload key".into()));
        }
        // Case-insensitive and Unicode-normalizing filesystems can otherwise
        // alias distinct logical keys. Runtime-generated keys are lowercase
        // ASCII hashes, so enforce that portable identity at this boundary.
        if !key.is_ascii() || key.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(ReclaimError::InvalidArgument(
                "payload key must be lowercase ASCII".into(),
            ));
        }
        if key.chars().any(|c| {
            c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
        }) || key.ends_with('.')
            || key.ends_with(' ')
        {
            return Err(ReclaimError::InvalidArgument(
                "payload key contains a reserved character".into(),
            ));
        }
        let stem = key
            .split('.')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let numbered_device = |prefix: &str| {
            stem.strip_prefix(prefix).is_some_and(|suffix| {
                suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9')
            })
        };
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$")
            || numbered_device("COM")
            || numbered_device("LPT")
        {
            return Err(ReclaimError::InvalidArgument(
                "payload key uses a reserved device name".into(),
            ));
        }
        Ok(())
    }

    fn path_for(&self, key: &str) -> Result<PathBuf> {
        Self::validate_key(key)?;
        Ok(self.root.join(key))
    }

    fn managed_file_name(name: &std::ffi::OsStr) -> bool {
        name.to_str()
            .is_some_and(|name| Self::validate_key(name).is_ok())
    }

    #[cfg(unix)]
    fn sync_root(&self) -> Result<()> {
        let directory = fs::File::open(&self.root).map_err(|e| {
            ReclaimError::Io(format!("opening backend dir {}: {e}", self.root.display()))
        })?;
        directory.sync_all().map_err(|e| {
            ReclaimError::Io(format!("syncing backend dir {}: {e}", self.root.display()))
        })
    }

    #[cfg(not(unix))]
    fn sync_root(&self) -> Result<()> {
        Ok(())
    }
}

impl Backend for FileBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<u64> {
        let path = self.path_for(key)?;
        let tmp = self
            .root
            .join(format!(".tmp-{key}-{}", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
                .map_err(|e| ReclaimError::Io(format!("creating {}: {e}", tmp.display())))?;
            file.write_all(data)
                .map_err(|e| ReclaimError::Io(format!("writing {}: {e}", tmp.display())))?;
            file.sync_all()
                .map_err(|e| ReclaimError::Io(format!("syncing {}: {e}", tmp.display())))?;
            drop(file);
            fs::rename(&tmp, &path).map_err(|e| {
                ReclaimError::Io(format!(
                    "renaming {} -> {}: {e}",
                    tmp.display(),
                    path.display()
                ))
            })?;
            self.sync_root()?;
            Ok(data.len() as u64)
        })();
        if result.is_err() {
            // Best-effort cleanup of a temp created by this call. The UUID and
            // create_new ensure it cannot name another writer's file.
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path_for(key)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(ReclaimError::Backend(format!(
                    "payload {key} is not a regular file"
                )))
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ReclaimError::NotFound(format!("payload {key}")))
            }
            Err(e) => {
                return Err(ReclaimError::Io(format!(
                    "reading metadata {}: {e}",
                    path.display()
                )))
            }
        }
        fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ReclaimError::NotFound(format!("payload {key}"))
            } else {
                ReclaimError::Io(format!("reading {}: {e}", path.display()))
            }
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key)?;
        match fs::remove_file(&path) {
            Ok(()) => self.sync_root(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ReclaimError::Io(format!(
                "deleting {}: {e}",
                path.display()
            ))),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let path = self.path_for(key)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => Ok(true),
            Ok(_) => Err(ReclaimError::Backend(format!(
                "payload {key} is not a regular file"
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(ReclaimError::Io(format!(
                "reading metadata {}: {error}",
                path.display()
            ))),
        }
    }

    fn verify(&self, key: &str, expected: &ContentHash) -> Result<()> {
        let data = self.get(key)?;
        crate::integrity::verify_sha256(&data, expected)
    }

    fn total_bytes(&self) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(&self.root) {
            for e in entries.flatten() {
                if Self::managed_file_name(&e.file_name())
                    && e.file_type().is_ok_and(|kind| kind.is_file())
                {
                    if let Ok(meta) = e.metadata() {
                        total = total.saturating_add(meta.len());
                    }
                }
            }
        }
        total
    }

    fn keys(&self) -> Vec<String> {
        let mut v: Vec<String> = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.root) {
            for e in entries.flatten() {
                if Self::managed_file_name(&e.file_name())
                    && e.file_type().is_ok_and(|kind| kind.is_file())
                {
                    if let Some(name) = e.file_name().to_str() {
                        v.push(name.to_string());
                    }
                }
            }
        }
        v.sort();
        v
    }

    fn kind(&self) -> &'static str {
        "file"
    }
}

/// Registry mapping backend id -> backend.
#[derive(Clone)]
pub struct BackendRegistry {
    backends: Arc<Mutex<HashMap<String, Arc<dyn Backend>>>>,
}

impl BackendRegistry {
    pub fn new() -> BackendRegistry {
        BackendRegistry {
            backends: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, backend: Arc<dyn Backend>) -> Result<()> {
        let id = backend.id().to_string();
        self.register_as(&id, backend)
    }

    /// Register a backend under an explicit key (used by nodes to namespace
    /// backend ids per process).
    pub fn register_as(&self, id: &str, backend: Arc<dyn Backend>) -> Result<()> {
        if id.is_empty() || id.trim() != id {
            return Err(ReclaimError::InvalidArgument(
                "backend id must be non-empty and have no surrounding whitespace".into(),
            ));
        }
        let mut backends = self
            .backends
            .lock()
            .map_err(|_| ReclaimError::Backend("backend registry poisoned".into()))?;
        if let Some(existing) = backends.get(id) {
            if Arc::ptr_eq(existing, &backend) {
                return Ok(());
            }
            return Err(ReclaimError::Backend(format!(
                "backend id {id:?} is already registered to a different backend"
            )));
        }
        backends.insert(id.to_string(), backend);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Arc<dyn Backend>> {
        self.backends
            .lock()
            .map_err(|_| ReclaimError::Backend("backend registry poisoned".into()))?
            .get(id)
            .cloned()
            .ok_or_else(|| ReclaimError::Backend(format!("unknown backend {id}")))
    }

    pub fn ids(&self) -> Result<Vec<String>> {
        let mut v: Vec<String> = self
            .backends
            .lock()
            .map_err(|_| ReclaimError::Backend("backend registry poisoned".into()))?
            .keys()
            .cloned()
            .collect();
        v.sort();
        Ok(v)
    }

    pub fn total_bytes(&self) -> Result<u64> {
        Ok(self
            .backends
            .lock()
            .map_err(|_| ReclaimError::Backend("backend registry poisoned".into()))?
            .values()
            .fold(0u64, |total, backend| {
                total.saturating_add(backend.total_bytes())
            }))
    }
}

impl Default for BackendRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_roundtrip() {
        let b = MemoryBackend::new("mem-test");
        b.put("k", b"hello").unwrap();
        assert_eq!(b.get("k").unwrap(), b"hello");
        assert_eq!(b.total_bytes(), 5);
        assert!(b.exists("k").unwrap());
        b.delete("k").unwrap();
        assert!(!b.exists("k").unwrap());
        assert!(b.get("k").is_err());
    }

    #[test]
    fn file_roundtrip_and_durability() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::new("f", dir.path().join("store")).unwrap();
        b.put("obj-a", b"payload-1").unwrap();
        assert_eq!(b.get("obj-a").unwrap(), b"payload-1");
        // Reopen: data must survive.
        let b2 = FileBackend::new("f", dir.path().join("store")).unwrap();
        assert_eq!(b2.get("obj-a").unwrap(), b"payload-1");
        b2.put("obj-a", b"replacement").unwrap();
        assert_eq!(b2.get("obj-a").unwrap(), b"replacement");
        b2.delete("obj-a").unwrap();
        assert!(!b2.exists("obj-a").unwrap());
        // Delete is idempotent.
        b2.delete("obj-a").unwrap();
    }

    #[test]
    fn backend_root_is_canonicalized_once() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("parent")).unwrap();
        let requested = dir.path().join("parent").join("..").join("store");
        let b = FileBackend::new("f", requested).unwrap();
        assert!(b.root.is_absolute());
        assert!(!b
            .root
            .components()
            .any(|component| { matches!(component, std::path::Component::ParentDir) }));
    }

    #[test]
    fn path_traversal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::new("f", dir.path()).unwrap();
        assert!(b.put("../escape", b"x").is_err());
        assert!(b.put("a/../b", b"x").is_err());
        assert!(b.put("a\\b", b"x").is_err());
        assert!(b.put("a/b", b"x").is_err());
        assert!(b.put(".", b"x").is_err());
        assert!(b.put(".tmp-owned", b"x").is_err());
        assert!(b.put("file:stream", b"x").is_err());
        assert!(b.put("CON", b"x").is_err());
        assert!(b.put("con", b"x").is_err());
        assert!(b.put("Uppercase", b"x").is_err());
        assert!(b.put("café", b"x").is_err());
        assert!(b.put("", b"x").is_err());
        assert!(b.put("ok-key", b"x").is_ok());
    }

    #[test]
    fn failed_put_cleans_its_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::new("f", dir.path()).unwrap();
        fs::create_dir(dir.path().join("occupied")).unwrap();
        assert!(b.put("occupied", b"data").is_err());
        assert!(b.exists("occupied").is_err());
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".tmp-"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn poisoned_memory_backend_existence_check_fails_closed() {
        let backend = Arc::new(MemoryBackend::new("memory"));
        let poison = Arc::clone(&backend);
        let _ = std::thread::spawn(move || {
            let _guard = poison.data.lock().unwrap();
            panic!("poison backend lock");
        })
        .join();
        assert!(backend.exists("missing").is_err());
    }

    #[test]
    fn temporary_and_foreign_files_are_not_payload_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::new("f", dir.path()).unwrap();
        b.put("managed", b"1234").unwrap();
        fs::write(dir.path().join(".tmp-stale"), b"stale").unwrap();
        // Uppercase is outside the portable managed-key domain.
        fs::write(dir.path().join("FOREIGN"), b"foreign").unwrap();
        assert_eq!(b.keys(), vec!["managed"]);
        assert_eq!(b.total_bytes(), 4);
    }

    #[test]
    fn concurrent_same_key_writes_leave_one_complete_payload() {
        let dir = tempfile::tempdir().unwrap();
        let b = Arc::new(FileBackend::new("f", dir.path()).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let payloads: Vec<Vec<u8>> = (0..4)
            .map(|i| format!("payload-{i}").into_bytes())
            .collect();
        let threads: Vec<_> = payloads
            .clone()
            .into_iter()
            .map(|payload| {
                let b = Arc::clone(&b);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    b.put("shared", &payload).unwrap();
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let stored = b.get("shared").unwrap();
        assert!(payloads.contains(&stored));
        assert_eq!(b.keys(), vec!["shared"]);
    }

    #[test]
    fn verify_integrity() {
        let b = MemoryBackend::new("mem-test");
        b.put("k", b"data").unwrap();
        b.verify("k", &ContentHash::of(b"data")).unwrap();
        assert!(b.verify("k", &ContentHash::of(b"other")).is_err());
    }

    #[test]
    fn registry_lookup() {
        let reg = BackendRegistry::new();
        reg.register(Arc::new(MemoryBackend::new("m1"))).unwrap();
        reg.register(Arc::new(MemoryBackend::new("m2"))).unwrap();
        assert_eq!(reg.ids().unwrap(), vec!["m1".to_string(), "m2".to_string()]);
        assert!(reg.get("nope").is_err());
    }

    #[test]
    fn poisoned_registry_listing_and_accounting_fail_closed() {
        let registry = BackendRegistry::new();
        let poison = registry.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison.backends.lock().unwrap();
            panic!("poison backend registry");
        })
        .join();
        assert!(registry.ids().is_err());
        assert!(registry.total_bytes().is_err());
    }

    #[test]
    fn duplicate_backend_id_cannot_retarget_existing_state() {
        let reg = BackendRegistry::new();
        let original: Arc<dyn Backend> = Arc::new(MemoryBackend::new("same"));
        original.put("payload", b"original").unwrap();
        reg.register(Arc::clone(&original)).unwrap();

        // Re-registering the exact same handle is an idempotent no-op.
        reg.register(Arc::clone(&original)).unwrap();

        let replacement: Arc<dyn Backend> = Arc::new(MemoryBackend::new("same"));
        replacement.put("payload", b"replacement").unwrap();
        assert!(reg.register(replacement).is_err());
        assert_eq!(
            reg.get("same").unwrap().get("payload").unwrap(),
            b"original"
        );
        assert!(Arc::ptr_eq(&reg.get("same").unwrap(), &original));
    }

    #[test]
    fn explicit_alias_rejects_empty_and_conflicting_ids() {
        let reg = BackendRegistry::new();
        let original: Arc<dyn Backend> = Arc::new(MemoryBackend::new("internal"));
        reg.register_as("alias", Arc::clone(&original)).unwrap();
        reg.register_as("alias", Arc::clone(&original)).unwrap();
        assert!(reg
            .register_as("alias", Arc::new(MemoryBackend::new("other")))
            .is_err());
        assert!(reg.register_as("", original).is_err());
    }
}
