//! Shared helpers for Reclaim Fabric examples.
#![allow(dead_code)]

use std::sync::Arc;

use reclaim_fabric::archive::LocalFsArchive;
use reclaim_fabric::backends::{BackendRegistry, FileBackend, MemoryBackend};
use reclaim_fabric::coordinator::{Clock, Coordinator, CoordinatorConfig, SystemClock};
use reclaim_fabric::errors::Result;
use reclaim_fabric::pressure::PressureRegistry;
use reclaim_fabric::protocol::{CreateObjectRequest, ReclaimRequest};
use tempfile::TempDir;
use uuid::Uuid;

/// A temporary single-process coordinator with memory + file backends and a
/// durable local archive.
pub struct Harness {
    pub _dir: TempDir,
    pub coordinator: Coordinator,
}

pub fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let backends = BackendRegistry::new();
    backends
        .register(Arc::new(MemoryBackend::new("memory")))
        .unwrap();
    let file_dir = dir.path().join("data");
    let backend = FileBackend::new("file:test", &file_dir).unwrap();
    backends.register(Arc::new(backend)).unwrap();
    let pressure = PressureRegistry::new();
    let archive_dir = dir.path().join("archive");
    let archive = LocalFsArchive::new("local-fs", &archive_dir).unwrap();
    let config = CoordinatorConfig {
        store_path,
        process_id: "example-coordinator".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some("127.0.0.1:9999".into()),
    };
    let coordinator = Coordinator::open(
        config,
        backends,
        pressure,
        vec![Arc::new(archive)],
        Arc::new(SystemClock),
    )
    .unwrap();
    Harness {
        _dir: dir,
        coordinator,
    }
}

/// Build a checkpoint-style object with explicit economics.
pub fn checkpoint(class: &str) -> reclaim_fabric::object::ReclaimObject {
    let now = SystemClock.now_ms();
    let mut o = reclaim_fabric::object::ReclaimObject::new(Uuid::new_v4(), 0, class, 4096, now);
    o.reuse_probability = 0.01;
    o.recompute_cost = Some(100.0);
    o.memory_cost_per_byte_sec = 0.5;
    o.storage_cost_per_byte_sec = 0.01;
    o
}

/// Create an object with a payload stored on the memory backend.
pub fn create_with_payload(
    coordinator: &Coordinator,
    obj: reclaim_fabric::object::ReclaimObject,
    payload: &[u8],
) -> Result<reclaim_fabric::object::ReclaimObject> {
    let req = CreateObjectRequest {
        object: obj,
        payload_b64: Some(reclaim_fabric::base64_payload(payload)),
        target_backend: Some("memory".into()),
        replicate_to: vec![],
    };
    coordinator.create_object(&req)
}

pub fn reclaim(
    coordinator: &Coordinator,
    id: Uuid,
    force: bool,
) -> Result<reclaim_fabric::coordinator::ReclaimReport> {
    coordinator.reclaim(&ReclaimRequest {
        object_id: id,
        actor: "example".into(),
        force,
    })
}
