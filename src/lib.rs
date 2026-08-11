//! # Reclaim Fabric
//!
//! A vendor-neutral machine-state reclamation runtime for AI infrastructure.
//!
//! Reclaim Fabric answers the question:
//!
//! > **What state is still worth keeping?**
//!
//! It is a distinct systems runtime with its own authority, state machine,
//! economics, persistence, recovery, transport, CLI, examples, tests,
//! benchmarks, and documentation. It is *not* a cache library, garbage
//! collector, LRU implementation, storage-tiering wrapper, model-serving
//! framework, or model-specific KV cache.
//!
//! The stack it complements:
//!
//! - **FlashTier** — where do the bytes live? (physical byte residency)
//! - **Context Fabric** — where does accumulated reusable computation live?
//! - **Compute Fabric** — where should the next computation run? (execution placement)
//! - **Reclaim Fabric** — what state is still worth keeping? (state lifecycle and reclamation)
//!
//! ## Library usage
//!
//! The runtime is usable directly without shelling out:
//!
//! ```no_run
//! use std::sync::Arc;
//! use reclaim_fabric::backends::{BackendRegistry, MemoryBackend};
//! use reclaim_fabric::coordinator::{Coordinator, CoordinatorConfig, SystemClock};
//! use reclaim_fabric::object::ReclaimObject;
//! use reclaim_fabric::pressure::PressureRegistry;
//! use reclaim_fabric::protocol::CreateObjectRequest;
//! use uuid::Uuid;
//!
//! # fn main() -> reclaim_fabric::errors::Result<()> {
//! let mut backends = BackendRegistry::new();
//! backends.register(Arc::new(MemoryBackend::new("memory")))?;
//! let pressure = PressureRegistry::new();
//! let mut config = CoordinatorConfig::default();
//! config.store_path = ":memory:".into();
//! let coordinator = Coordinator::open(config, backends, pressure, vec![], Arc::new(SystemClock))?;
//!
//! let mut obj = ReclaimObject::new(Uuid::new_v4(), 0, "checkpoint", 1024, coordinator.now_ms());
//! obj.recompute_cost = Some(5000.0);
//! let req = CreateObjectRequest {
//!     object: obj.clone(),
//!     payload_b64: Some(reclaim_fabric::base64_payload(b"state")),
//!     target_backend: Some("memory".into()),
//!     replicate_to: vec![],
//! };
//! let created = coordinator.create_object(&req)?;
//! assert_eq!(created.lifecycle_state, reclaim_fabric::lifecycle::LifecycleState::Hot);
//! # Ok(())
//! # }
//! ```

pub mod archive;
pub mod audit;
pub mod backends;
pub mod cli;
pub mod compression;
pub mod coordinator;
pub mod dedup;
pub mod economics;
pub mod errors;
pub mod integrity;
pub mod lifecycle;
pub mod lineage;
pub mod node;
pub mod object;
pub mod persistence;
pub mod policy;
pub mod pressure;
pub mod protocol;
pub mod recovery;
pub mod transport;

/// Encode a payload for `CreateObjectRequest.payload_b64`.
pub fn base64_payload(data: &[u8]) -> String {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    STANDARD.encode(data)
}

/// Decode a base64 payload from a `CreateObjectRequest`.
pub fn decode_payload_b64(s: &str) -> errors::Result<Vec<u8>> {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    STANDARD
        .decode(s)
        .map_err(|e| errors::ReclaimError::InvalidArgument(format!("bad base64: {e}")))
}

pub use errors::{ReclaimError, Result, WireError};
