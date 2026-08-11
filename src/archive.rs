//! Archival abstraction.
//!
//! `ArchiveBackend` stores immutable, integrity-checked byte blobs. The
//! built-in backend is a durable local filesystem archive with immutable,
//! atomic no-replace publication (fsynced temp file + hard link). Future backends (NVMe pools, object storage,
//! distributed stores) plug into the same trait.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;

/// Result of an archival write.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArchiveRecord {
    pub archive_id: String,
    pub object_id: uuid::Uuid,
    pub generation: u64,
    pub backend: String,
    pub key: String,
    pub size: u64,
    pub content_hash: ContentHash,
    pub created_at_ms: i64,
}

/// Archive backend trait.
pub trait ArchiveBackend: Send + Sync {
    fn id(&self) -> &str;

    /// Write `data` under `key` atomically and verify integrity before
    /// returning Ok. Partial writes are never visible as valid archives.
    fn write(&self, key: &str, data: &[u8], expected_hash: &ContentHash) -> Result<u64>;

    /// Read archived bytes.
    fn read(&self, key: &str) -> Result<Vec<u8>>;

    /// Verify integrity of an archived blob.
    fn verify(&self, key: &str, expected_hash: &ContentHash) -> Result<()>;

    /// Delete an archived blob (idempotent).
    fn delete(&self, key: &str) -> Result<()>;

    /// Check whether an archive exists. Metadata/I/O failures are surfaced so
    /// recovery cannot mistake an inaccessible archive for a deleted one.
    fn exists(&self, key: &str) -> Result<bool>;

    fn total_bytes(&self) -> u64;
}

/// Durable local filesystem archive backend.
pub struct LocalFsArchive {
    id: String,
    root: PathBuf,
}

struct TempArchiveFile(PathBuf);

impl Drop for TempArchiveFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| ReclaimError::Io(format!("archive directory sync {}: {e}", path.display())))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    // Windows does not expose portable directory handles through std. The
    // payload handle itself is flushed before its name is published.
    Ok(())
}

impl LocalFsArchive {
    pub fn new(id: impl Into<String>, root: impl AsRef<Path>) -> Result<LocalFsArchive> {
        let id = id.into();
        if id.is_empty() {
            return Err(ReclaimError::InvalidArgument(
                "archive backend id must not be empty".into(),
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| {
            ReclaimError::Io(format!("creating archive dir {}: {e}", root.display()))
        })?;
        // Anchor all later operations to the directory opened now. Retaining
        // a relative root would silently redirect reads/deletes if any host
        // code later changed the process-wide current directory.
        let root = fs::canonicalize(&root).map_err(|e| {
            ReclaimError::Io(format!("resolving archive dir {}: {e}", root.display()))
        })?;
        Ok(LocalFsArchive { id, root })
    }

    fn validate_key(key: &str) -> Result<()> {
        // The runtime generates lowercase ASCII UUID/generation paths. Keep
        // the public boundary in that portable domain because the legacy
        // compatibility path forms a literal filename, where Windows ADS,
        // device names, case folding, and Unicode normalization can alias.
        if key.is_empty() || key.len() > 200 || !key.is_ascii() {
            return Err(ReclaimError::ArchiveFailure(format!(
                "invalid or traversal archive key: {key:?}"
            )));
        }
        if key.starts_with('/')
            || key.starts_with('\\')
            || key.contains('\\')
            || key.bytes().any(|byte| byte.is_ascii_uppercase())
            || key
                .chars()
                .any(|c| c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        {
            return Err(ReclaimError::ArchiveFailure(format!(
                "archive key is not a portable lowercase ASCII path: {key:?}"
            )));
        }
        for component in key.split('/') {
            if component.is_empty()
                || component == "."
                || component == ".."
                || component.ends_with('.')
                || component.ends_with(' ')
            {
                return Err(ReclaimError::ArchiveFailure(format!(
                    "archive key contains a reserved path component: {key:?}"
                )));
            }
            let stem = component
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
                return Err(ReclaimError::ArchiveFailure(format!(
                    "archive key uses a reserved device name: {key:?}"
                )));
            }
        }
        if key.replace('/', "-").starts_with(".tmp-archive-") {
            return Err(ReclaimError::ArchiveFailure(
                "archive key aliases the reserved temporary-file namespace".into(),
            ));
        }
        Ok(())
    }

    fn path_for(&self, key: &str) -> Result<PathBuf> {
        Self::validate_key(key)?;
        // Hash the opaque logical key into a flat, platform-neutral filename.
        // Replacing separators (the old approach) aliases e.g. `a/b` and
        // `a-b`, allowing one logical archive to overwrite another on Unix
        // while merely failing on Windows.
        Ok(self
            .root
            .join(format!("blob-{}", ContentHash::of(key.as_bytes()))))
    }

    fn legacy_path_for(&self, key: &str) -> Result<PathBuf> {
        // Releases predating the collision-safe filename mapping flattened
        // `/` to `-`. Keep a read/delete compatibility path so a restart does
        // not strand already-persisted archives. All new publication uses the
        // collision-safe hashed path above.
        self.path_for(key)?;
        Ok(self.root.join(key.replace('/', "-")))
    }

    fn read_regular_file(&self, path: &Path, operation: &str) -> Result<Vec<u8>> {
        let metadata = fs::symlink_metadata(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ReclaimError::NotFound(format!("archive file {}", path.display()))
            } else {
                ReclaimError::Io(format!("archive {operation} {}: {e}", path.display()))
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ReclaimError::ArchiveFailure(format!(
                "archive {operation} refused non-regular file {}",
                path.display()
            )));
        }
        fs::read(path)
            .map_err(|e| ReclaimError::Io(format!("archive {operation} {}: {e}", path.display())))
    }

    fn cleanup_failed_publication(
        &self,
        path: &Path,
        cause: impl std::fmt::Display,
    ) -> ReclaimError {
        let mut cleanup_errors = Vec::new();
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => cleanup_errors.push(format!("remove {}: {e}", path.display())),
        }
        if let Err(e) = sync_directory(&self.root) {
            cleanup_errors.push(e.to_string());
        }
        if cleanup_errors.is_empty() {
            ReclaimError::ArchiveFailure(format!("{cause}"))
        } else {
            ReclaimError::ArchiveFailure(format!(
                "{cause}; failed-publication cleanup also failed: {}",
                cleanup_errors.join("; ")
            ))
        }
    }
}

impl ArchiveBackend for LocalFsArchive {
    fn id(&self) -> &str {
        &self.id
    }

    fn write(&self, key: &str, data: &[u8], expected_hash: &ContentHash) -> Result<u64> {
        let size = u64::try_from(data.len()).map_err(|_| {
            ReclaimError::ArchiveFailure("archive payload length exceeds u64".into())
        })?;
        // Integrity must be provable *before* we expose the archive record.
        crate::integrity::verify_sha256(data, expected_hash).map_err(|e| {
            ReclaimError::ArchiveFailure(format!("refusing to archive corrupt payload: {e}"))
        })?;
        let final_path = self.path_for(key)?;
        if final_path.exists() {
            let existing = self.read_regular_file(&final_path, "existing read")?;
            crate::integrity::verify_sha256(&existing, expected_hash).map_err(|e| {
                ReclaimError::ArchiveFailure(format!(
                    "archive key {key:?} already contains different data: {e}"
                ))
            })?;
            if existing != data {
                return Err(ReclaimError::ArchiveFailure(format!(
                    "archive key {key:?} content collision"
                )));
            }
            return Ok(size);
        }
        let legacy_path = self.legacy_path_for(key)?;
        if legacy_path.exists() {
            let existing = self.read_regular_file(&legacy_path, "legacy read")?;
            if crate::integrity::verify_sha256(&existing, expected_hash).is_ok() && existing == data
            {
                return Ok(size);
            }
            // A different logical key may alias the same legacy flattened
            // name. Leave it untouched and publish this key under the new,
            // collision-safe name.
        }
        let key_hash = ContentHash::of(key.as_bytes());
        let tmp = self.root.join(format!(
            ".tmp-archive-{}-{}",
            key_hash,
            uuid::Uuid::new_v4()
        ));
        let temp_guard = TempArchiveFile(tmp.clone());
        let mut f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_guard.0)
            .map_err(|e| ReclaimError::Io(format!("archive open {}: {e}", tmp.display())))?;
        f.write_all(data)
            .map_err(|e| ReclaimError::Io(format!("archive write {}: {e}", tmp.display())))?;
        f.sync_all()
            .map_err(|e| ReclaimError::Io(format!("archive sync {}: {e}", tmp.display())))?;
        drop(f);

        // hard_link is an atomic create-without-replacement operation on the
        // same filesystem. Unlike rename, it never overwrites an immutable
        // archive and has consistent collision behavior on Windows and Unix.
        match fs::hard_link(&temp_guard.0, &final_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self.read_regular_file(&final_path, "race read")?;
                crate::integrity::verify_sha256(&existing, expected_hash).map_err(|verify_error| {
                    ReclaimError::ArchiveFailure(format!(
                        "archive key {key:?} concurrently created with different data: {verify_error}"
                    ))
                })?;
                if existing != data {
                    return Err(ReclaimError::ArchiveFailure(format!(
                        "archive key {key:?} concurrent content collision"
                    )));
                }
                return Ok(size);
            }
            Err(e) => {
                return Err(ReclaimError::ArchiveFailure(format!(
                    "archive publish {} -> {}: {e}",
                    tmp.display(),
                    final_path.display()
                )))
            }
        }
        if let Err(e) = sync_directory(&self.root) {
            return Err(self.cleanup_failed_publication(&final_path, e));
        }
        // Re-read and verify after publication to catch silent corruption.
        let written = self
            .read_regular_file(&final_path, "verification read")
            .map_err(|e| self.cleanup_failed_publication(&final_path, e))?;
        if let Err(e) = crate::integrity::verify_sha256(&written, expected_hash) {
            return Err(self.cleanup_failed_publication(
                &final_path,
                format_args!("archive verification after publication failed: {e}"),
            ));
        }
        drop(temp_guard);
        Ok(size)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path_for(key)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => self.read_regular_file(&path, &format!("read for key {key:?}")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let legacy = self.legacy_path_for(key)?;
                self.read_regular_file(&legacy, &format!("legacy read for key {key:?}"))
            }
            Err(e) => Err(ReclaimError::Io(format!(
                "archive metadata read {}: {e}",
                path.display()
            ))),
        }
    }

    fn verify(&self, key: &str, expected_hash: &ContentHash) -> Result<()> {
        let data = self.read(key)?;
        crate::integrity::verify_sha256(&data, expected_hash)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key)?;
        let legacy = self.legacy_path_for(key)?;
        let mut removed = false;
        for candidate in [&path, &legacy] {
            match fs::remove_file(candidate) {
                Ok(()) => removed = true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(ReclaimError::Io(format!(
                        "archive delete {}: {e}",
                        candidate.display()
                    )))
                }
            }
        }
        if removed {
            sync_directory(&self.root)
        } else {
            Ok(())
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let paths = [self.path_for(key)?, self.legacy_path_for(key)?];
        for path in paths {
            match fs::symlink_metadata(&path) {
                Ok(metadata)
                    if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
                {
                    return Ok(true)
                }
                Ok(_) => {
                    return Err(ReclaimError::ArchiveFailure(format!(
                        "archive {} is not a regular file",
                        path.display()
                    )))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(ReclaimError::Io(format!(
                        "reading archive metadata {}: {error}",
                        path.display()
                    )))
                }
            }
        }
        Ok(false)
    }

    fn total_bytes(&self) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(&self.root) {
            for e in entries.flatten() {
                if let Ok(meta) = fs::symlink_metadata(e.path()) {
                    if meta.is_file()
                        && !meta.file_type().is_symlink()
                        && !e.file_name().to_string_lossy().starts_with(".tmp-archive-")
                    {
                        total = total.saturating_add(meta.len());
                    }
                }
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalFsArchive::new("local", dir.path().join("arch")).unwrap();
        let data = b"archive payload".to_vec();
        let hash = ContentHash::of(&data);
        let size = a.write("obj-1/gen-0", &data, &hash).unwrap();
        assert_eq!(size, data.len() as u64);
        assert!(a.exists("obj-1/gen-0").unwrap());
        a.verify("obj-1/gen-0", &hash).unwrap();
        assert_eq!(a.read("obj-1/gen-0").unwrap(), data);
        // Reopen: persists.
        let a2 = LocalFsArchive::new("local", dir.path().join("arch")).unwrap();
        a2.verify("obj-1/gen-0", &hash).unwrap();
        a2.delete("obj-1/gen-0").unwrap();
        assert!(!a2.exists("obj-1/gen-0").unwrap());
        a2.delete("obj-1/gen-0").unwrap(); // idempotent
    }

    #[test]
    fn archive_root_is_canonicalized_once() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("parent")).unwrap();
        let requested = dir.path().join("parent").join("..").join("arch");
        let archive = LocalFsArchive::new("local", requested).unwrap();
        assert!(archive.root.is_absolute());
        assert!(!archive
            .root
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)));
    }

    #[test]
    fn corrupt_payload_refused() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalFsArchive::new("local", dir.path().join("arch")).unwrap();
        let data = b"payload".to_vec();
        let wrong = ContentHash::of(b"different");
        assert!(a.write("k", &data, &wrong).is_err());
        assert!(!a.exists("k").unwrap());
    }

    #[test]
    fn traversal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalFsArchive::new("local", dir.path().join("arch")).unwrap();
        assert!(a.write("../escape", b"x", &ContentHash::of(b"x")).is_err());
        assert!(a.write("a/../b", b"x", &ContentHash::of(b"x")).is_err());
        for key in [
            "file:stream",
            "nul",
            "con.txt",
            "a/com1/b",
            "a//b",
            "a/b.",
            ".tmp-archive-owned",
            "Uppercase",
            "café",
            "control\0byte",
        ] {
            assert!(
                a.write(key, b"x", &ContentHash::of(b"x")).is_err(),
                "unsafe archive key unexpectedly accepted: {key:?}"
            );
            assert!(a.exists(key).is_err());
        }
        assert!(a.write("valid/key-1", b"x", &ContentHash::of(b"x")).is_ok());
    }

    #[test]
    fn archive_existence_check_rejects_non_file_entries() {
        let dir = tempfile::tempdir().unwrap();
        let archive = LocalFsArchive::new("a", dir.path()).unwrap();
        let path = archive.path_for("occupied").unwrap();
        fs::create_dir(path).unwrap();
        assert!(archive.exists("occupied").is_err());
    }

    #[test]
    fn separator_replacement_cannot_alias_distinct_keys() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalFsArchive::new("local", dir.path().join("arch")).unwrap();
        let slash = b"slash payload";
        let dash = b"dash payload";
        a.write("a/b", slash, &ContentHash::of(slash)).unwrap();
        a.write("a-b", dash, &ContentHash::of(dash)).unwrap();
        assert_eq!(a.read("a/b").unwrap(), slash);
        assert_eq!(a.read("a-b").unwrap(), dash);
    }

    #[test]
    fn legacy_flattened_archive_remains_readable_without_blocking_alias_fix() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("arch");
        let a = LocalFsArchive::new("local", &root).unwrap();
        fs::write(root.join("a-b"), b"legacy slash").unwrap();

        // The old a/b record remains readable through the compatibility path.
        assert_eq!(a.read("a/b").unwrap(), b"legacy slash");
        // A new key that used to alias the same filename gets its own hashed
        // publication and cannot overwrite the legacy record.
        a.write("a-b", b"new dash", &ContentHash::of(b"new dash"))
            .unwrap();
        assert_eq!(a.read("a-b").unwrap(), b"new dash");
        assert_eq!(a.read("a/b").unwrap(), b"legacy slash");
    }

    #[test]
    fn archive_key_is_immutable_and_failed_rewrite_preserves_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalFsArchive::new("local", dir.path().join("arch")).unwrap();
        a.write("same", b"first", &ContentHash::of(b"first"))
            .unwrap();
        assert!(a
            .write("same", b"second", &ContentHash::of(b"second"))
            .is_err());
        assert_eq!(a.read("same").unwrap(), b"first");
    }

    #[test]
    fn concurrent_writers_never_replace_or_publish_partial_data() {
        let dir = tempfile::tempdir().unwrap();
        let archive =
            std::sync::Arc::new(LocalFsArchive::new("local", dir.path().join("arch")).unwrap());
        for iteration in 0..32 {
            let key = format!("race/{iteration}");
            let left = format!("left-{iteration}").into_bytes();
            let right = format!("right-{iteration}").into_bytes();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let (left_result, right_result) = std::thread::scope(|scope| {
                let left_archive = archive.clone();
                let left_key = key.clone();
                let left_barrier = barrier.clone();
                let left_data = left.clone();
                let left = scope.spawn(move || {
                    left_barrier.wait();
                    left_archive.write(&left_key, &left_data, &ContentHash::of(&left_data))
                });
                let right_archive = archive.clone();
                let right_key = key.clone();
                let right_barrier = barrier.clone();
                let right_data = right.clone();
                let right = scope.spawn(move || {
                    right_barrier.wait();
                    right_archive.write(&right_key, &right_data, &ContentHash::of(&right_data))
                });
                (left.join().unwrap(), right.join().unwrap())
            });
            assert_ne!(left_result.is_ok(), right_result.is_ok());
            let stored = archive.read(&key).unwrap();
            assert!(stored == left || stored == right);
        }
        let temporary_count = fs::read_dir(dir.path().join("arch"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".tmp-archive-")
            })
            .count();
        assert_eq!(temporary_count, 0);
    }
}
