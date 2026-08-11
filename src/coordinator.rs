//! Coordinator: the authority for lifecycle decisions and reclamation.
//!
//! The coordinator owns:
//! - the durable store (objects, lineage, dedup, decisions, attempts,
//!   reservations, journal, audit)
//! - coordinator epochs (stale writers are rejected)
//! - policy resolution and deterministic decisions
//! - the transactional reclaim lifecycle:
//!   plan -> reserve -> validate -> execute -> verify -> commit
//! - orchestration of physical operations on nodes (remote) or local backends
//!
//! Physical operations never run while holding the coordinator lock; journal
//! phases make every step crash-recoverable.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;
use uuid::Uuid;

use crate::archive::{ArchiveBackend, ArchiveRecord};
use crate::backends::{Backend, BackendRegistry};
use crate::compression::{compress_verified_with_bytes, CompressionCodec, ZstdCodec};
use crate::dedup::{dedup_key, DedupEntry};
use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;
use crate::lifecycle::{LifecycleState, TransitionResult};
use crate::lineage::{EdgeKind, LineageGraph};
use crate::object::{DurabilityClass, PhysicalKind, ReclaimObject, Replica};
use crate::persistence::{
    Attempt, AttemptStatus, AuditEntry, FailureRecord, JournalEntry, JournalPhase, Reservation,
    Store,
};
use crate::policy::{DecisionContext, Policy, PolicyDecision, PolicyRegistry};
use crate::pressure::{PressureLevel, PressureMetrics, PressureRegistry};
use crate::protocol::{
    CreateObjectRequest, NodeOperationReply, NodeOperationRequest, NodeRegisterReply,
    NodeRegisterRequest, ReclaimRequest,
};
use crate::recovery::{journal_payload, reconcile_store};
use crate::transport::Client;

const COMPRESSION_ORIGINAL_SIZE_KEY: &str = "reclaim_fabric.internal.compression_original_size";

/// Injectable clock for deterministic tests.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    }
}

/// Frozen/injectable clock for tests and deterministic replay.
pub struct FrozenClock {
    now: Mutex<i64>,
}

impl FrozenClock {
    pub fn new(now_ms: i64) -> FrozenClock {
        FrozenClock {
            now: Mutex::new(now_ms),
        }
    }
    pub fn advance(&self, ms: i64) {
        let mut now = self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now = now.saturating_add(ms);
    }
    pub fn set(&self, ms: i64) {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ms;
    }
}

impl Clock for FrozenClock {
    fn now_ms(&self) -> i64 {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Node registration state held by the coordinator.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub name: String,
    pub process_id: String,
    pub boot_id: Uuid,
    pub addr: String,
    pub backends: Vec<crate::protocol::BackendDescriptor>,
    pub last_seen_ms: i64,
}

/// Coordinator configuration.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub store_path: String,
    pub process_id: String,
    pub reservation_ttl_ms: i64,
    pub node_heartbeat_timeout_ms: i64,
    pub node_addr: Option<String>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        CoordinatorConfig {
            store_path: "reclaim-fabric.db".into(),
            process_id: format!("coordinator-{}", std::process::id()),
            reservation_ttl_ms: 60_000,
            node_heartbeat_timeout_ms: 30_000,
            node_addr: None,
        }
    }
}

/// Report of a reclaim operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReclaimReport {
    pub object_id: Uuid,
    pub generation: u64,
    pub reclaimed: bool,
    pub prior_state: String,
    pub final_state: String,
    pub attempt_id: Uuid,
    pub reservation_id: Uuid,
    pub decision: Option<PolicyDecision>,
    pub explanation: Option<serde_json::Value>,
    pub deleted_replicas: Vec<Uuid>,
    pub deleted_archives: Vec<String>,
    pub physical_error: Option<String>,
}

struct CoordinatorInner {
    store: Store,
    epoch: u64,
    boot_id: Uuid,
    policies: PolicyRegistry,
    pressure: PressureRegistry,
    backends: BackendRegistry,
    archives: Vec<Arc<dyn ArchiveBackend>>,
    nodes: HashMap<String, NodeInfo>,
    /// In-process per-object exclusion for lifecycle operations that release
    /// the coordinator mutex while performing physical I/O.
    active_objects: HashSet<Uuid>,
    /// Serialize dedup metadata and its corresponding physical payload by
    /// (backend, content identity). This closes the write/acquire/delete race
    /// across different objects sharing the same bytes.
    active_payloads: HashSet<(String, ContentHash)>,
    /// Online recovery is globally exclusive with every object/payload
    /// operation because journal classification observes physical state while
    /// lifecycle methods temporarily release the coordinator mutex for I/O.
    active_recovery: bool,
    config: CoordinatorConfig,
}

/// The coordinator runtime.
pub struct Coordinator {
    inner: Mutex<CoordinatorInner>,
    epoch: u64,
    clock: Arc<dyn Clock>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    authority_lost: Arc<std::sync::atomic::AtomicBool>,
    heartbeat_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct ObjectOperationGuard<'a> {
    coordinator: &'a Coordinator,
    object_id: Uuid,
}

struct PayloadOperationGuard<'a> {
    coordinator: &'a Coordinator,
    keys: Vec<(String, ContentHash)>,
}

struct RecoveryOperationGuard<'a> {
    coordinator: &'a Coordinator,
}

impl Drop for ObjectOperationGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.coordinator.inner.lock() {
            inner.active_objects.remove(&self.object_id);
        }
    }
}

impl Drop for PayloadOperationGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.coordinator.inner.lock() {
            for key in &self.keys {
                inner.active_payloads.remove(key);
            }
        }
    }
}

impl Drop for RecoveryOperationGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.coordinator.inner.lock() {
            inner.active_recovery = false;
        }
    }
}

impl Coordinator {
    /// Open a store and claim authority. Runs recovery before accepting work.
    pub fn open(
        config: CoordinatorConfig,
        backends: BackendRegistry,
        pressure: PressureRegistry,
        archives: Vec<Arc<dyn ArchiveBackend>>,
        clock: Arc<dyn Clock>,
    ) -> Result<Coordinator> {
        if config.reservation_ttl_ms <= 0 {
            return Err(ReclaimError::InvalidArgument(
                "reservation TTL must be greater than zero".into(),
            ));
        }
        if config.node_heartbeat_timeout_ms <= 0 {
            return Err(ReclaimError::InvalidArgument(
                "node heartbeat timeout must be greater than zero".into(),
            ));
        }
        if config.process_id.trim().is_empty() || config.process_id.trim() != config.process_id {
            return Err(ReclaimError::InvalidArgument(
                "coordinator process id must be non-empty and have no surrounding whitespace"
                    .into(),
            ));
        }
        let mut archive_ids = HashSet::new();
        for archive in &archives {
            let id = archive.id();
            if id.trim().is_empty() || id.trim() != id {
                return Err(ReclaimError::InvalidArgument(
                    "archive backend ids must be non-empty and have no surrounding whitespace"
                        .into(),
                ));
            }
            if !archive_ids.insert(id.to_string()) {
                return Err(ReclaimError::InvalidArgument(format!(
                    "duplicate archive backend id {id}"
                )));
            }
        }
        let store = Store::open(&config.store_path)?;
        let persisted = store.list_policies()?;
        let seed_default_policies = persisted.is_empty();
        let policies = if seed_default_policies {
            PolicyRegistry::with_defaults()
        } else {
            let mut registry = PolicyRegistry::default();
            for p in persisted {
                registry.add(p)?;
            }
            registry
        };
        policies.validate_complete()?;

        let boot_id = Uuid::new_v4();
        let epoch = store.claim_coordinator(
            &config.process_id,
            &boot_id,
            clock.now_ms(),
            config.node_heartbeat_timeout_ms.max(5_000),
        )?;
        if seed_default_policies {
            let seed_result = (|| {
                store.upsert_policy(&crate::policy::default_policy())?;
                store.upsert_policy(&crate::policy::emergency_policy())?;
                Result::Ok(())
            })();
            if let Err(error) = seed_result {
                let _ = store.release_coordinator(&config.process_id, &boot_id, epoch);
                return Err(error);
            }
        }

        let coordinator = Coordinator {
            inner: Mutex::new(CoordinatorInner {
                store,
                epoch,
                boot_id,
                policies,
                pressure,
                backends,
                archives,
                nodes: HashMap::new(),
                active_objects: HashSet::new(),
                active_payloads: HashSet::new(),
                active_recovery: false,
                config,
            }),
            epoch,
            clock,
            shutdown: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            authority_lost: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            heartbeat_handle: Mutex::new(None),
        };

        // Recovery: reconcile the journal against physical truth before
        // accepting any new work.
        let report = coordinator.recover()?;
        log::info!(
            "recovery complete: committed={:?} rolled_back={:?} errors={:?}",
            report.committed,
            report.rolled_back,
            report.errors
        );
        if !report.errors.is_empty() {
            return Err(ReclaimError::Recovery(format!(
                "recovery left unresolved journal state: {}",
                report.errors.join("; ")
            )));
        }
        coordinator.start_authority_heartbeat()?;
        Ok(coordinator)
    }

    /// Request a graceful runtime shutdown (checked by the transport layer).
    pub fn request_shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Release coordinator authority after a clean shutdown so a restart can
    /// take over immediately (crashes leave the claim fresh and subject to
    /// the stale-window rule instead).
    pub fn release(&self) -> Result<()> {
        self.stop_authority_heartbeat()?;
        let guard = self
            .inner
            .lock()
            .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
        guard
            .store
            .release_coordinator(&guard.config.process_id, &guard.boot_id, guard.epoch)?;
        self.authority_lost
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn store(&self) -> Result<Store> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?
            .store
            .clone())
    }

    /// Fetch a registered backend by id (for integrators and tests).
    pub fn backend(&self, id: &str) -> Result<Arc<dyn Backend>> {
        self.with_inner(|g| g.backends.get(id))
    }

    /// Register an additional backend at runtime (for integrators and tests).
    pub fn register_backend(&self, id: &str, backend: Arc<dyn Backend>) -> Result<()> {
        self.with_inner(|g| g.backends.register_as(id, backend))
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    fn with_inner<T>(&self, f: impl FnOnce(&mut CoordinatorInner) -> Result<T>) -> Result<T> {
        if self
            .authority_lost
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ReclaimError::ReservationConflict(
                "coordinator authority has been lost".into(),
            ));
        }
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
        if let Err(e) = guard.store.coordinator_heartbeat(
            &guard.config.process_id,
            &guard.boot_id,
            guard.epoch,
            self.clock.now_ms(),
        ) {
            self.authority_lost
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(e);
        }
        f(&mut guard)
    }

    fn start_authority_heartbeat(&self) -> Result<()> {
        let (store, process_id, boot_id, epoch, interval_ms) = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
            let stale_ms = guard.config.node_heartbeat_timeout_ms.max(5_000) as u64;
            (
                guard.store.clone(),
                guard.config.process_id.clone(),
                guard.boot_id,
                guard.epoch,
                (stale_ms / 3).clamp(100, 5_000),
            )
        };
        let shutdown = self.shutdown.clone();
        let authority_lost = self.authority_lost.clone();
        let clock = self.clock.clone();
        let handle = std::thread::spawn(move || {
            while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                let mut slept = 0;
                while slept < interval_ms && !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    let step = (interval_ms - slept).min(100);
                    std::thread::sleep(std::time::Duration::from_millis(step));
                    slept += step;
                }
                if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                if let Err(e) =
                    store.coordinator_heartbeat(&process_id, &boot_id, epoch, clock.now_ms())
                {
                    log::error!("coordinator authority heartbeat failed: {e}");
                    authority_lost.store(true, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
            }
        });
        let mut slot = self
            .heartbeat_handle
            .lock()
            .map_err(|_| ReclaimError::Internal("coordinator heartbeat handle poisoned".into()))?;
        *slot = Some(handle);
        Ok(())
    }

    fn stop_authority_heartbeat(&self) -> Result<()> {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(12);
        loop {
            let finished = self
                .heartbeat_handle
                .lock()
                .map_err(|_| {
                    ReclaimError::Internal("coordinator heartbeat handle poisoned".into())
                })?
                .as_ref()
                .map(|h| h.is_finished())
                .unwrap_or(true);
            if finished {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(ReclaimError::Internal(
                    "coordinator heartbeat thread did not stop before deadline".into(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        if let Some(handle) = self
            .heartbeat_handle
            .lock()
            .map_err(|_| ReclaimError::Internal("coordinator heartbeat handle poisoned".into()))?
            .take()
        {
            handle.join().map_err(|_| {
                ReclaimError::Internal("coordinator heartbeat thread panicked".into())
            })?;
        }
        Ok(())
    }

    fn begin_object_operation(&self, id: &Uuid) -> Result<ObjectOperationGuard<'_>> {
        self.with_inner(|inner| {
            if inner.active_recovery {
                return Err(ReclaimError::ReservationConflict(
                    "recovery is in progress".into(),
                ));
            }
            if !inner.active_objects.insert(*id) {
                return Err(ReclaimError::ReservationConflict(format!(
                    "object {id} already has an operation in progress"
                )));
            }
            Ok(())
        })?;
        Ok(ObjectOperationGuard {
            coordinator: self,
            object_id: *id,
        })
    }

    fn begin_payload_operations(
        &self,
        mut keys: Vec<(String, ContentHash)>,
    ) -> Result<PayloadOperationGuard<'_>> {
        keys.sort();
        keys.dedup();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if self.shutdown_requested() {
                return Err(ReclaimError::ReservationConflict(
                    "coordinator is shutting down".into(),
                ));
            }
            if self
                .authority_lost
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ReclaimError::ReservationConflict(
                    "coordinator authority has been lost".into(),
                ));
            }
            let acquired = {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
                if !inner.active_recovery
                    && keys.iter().all(|key| !inner.active_payloads.contains(key))
                {
                    inner.active_payloads.extend(keys.iter().cloned());
                    true
                } else {
                    false
                }
            };
            if acquired {
                return Ok(PayloadOperationGuard {
                    coordinator: self,
                    keys,
                });
            }
            if std::time::Instant::now() >= deadline {
                return Err(ReclaimError::ReservationConflict(
                    "timed out waiting for shared payload operation".into(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn begin_recovery(&self) -> Result<RecoveryOperationGuard<'_>> {
        self.with_inner(|inner| {
            if inner.active_recovery
                || !inner.active_objects.is_empty()
                || !inner.active_payloads.is_empty()
            {
                return Err(ReclaimError::ReservationConflict(
                    "cannot run recovery while lifecycle operations are active".into(),
                ));
            }
            inner.active_recovery = true;
            Ok(())
        })?;
        Ok(RecoveryOperationGuard { coordinator: self })
    }

    // ------------------------------------------------------------------
    // Objects
    // ------------------------------------------------------------------

    /// Register a new object, optionally storing its payload.
    pub fn create_object(&self, req: &CreateObjectRequest) -> Result<ReclaimObject> {
        let mut obj = req.object.clone();
        obj.validate()?;
        let _operation = self.begin_object_operation(&obj.id)?;
        let prepared_payload = match &req.payload_b64 {
            Some(payload_b64) => {
                let target = req.target_backend.clone().ok_or_else(|| {
                    ReclaimError::InvalidArgument(
                        "target_backend is required when payload_b64 is present".into(),
                    )
                })?;
                if target.trim().is_empty() {
                    return Err(ReclaimError::InvalidArgument(
                        "target_backend must not be empty".into(),
                    ));
                }
                if req.replicate_to.iter().any(|id| id.trim().is_empty()) {
                    return Err(ReclaimError::InvalidArgument(
                        "replicate_to backend ids must not be empty".into(),
                    ));
                }
                let data = decode_base64(payload_b64)?;
                if data.is_empty() {
                    return Err(ReclaimError::InvalidArgument(
                        "empty payloads are not supported".into(),
                    ));
                }
                let hash = ContentHash::of(&data);
                if let Some(expected) = obj.content_hash {
                    if expected != hash {
                        return Err(ReclaimError::IntegrityFailure(format!(
                            "payload does not match declared content hash: declared {expected}, actual {hash}"
                        )));
                    }
                }
                if obj.physical_size != 0 && obj.physical_size != data.len() as u64 {
                    return Err(ReclaimError::InvalidArgument(format!(
                        "declared physical size {} does not match payload size {}",
                        obj.physical_size,
                        data.len()
                    )));
                }
                let mut targets = vec![target.clone()];
                targets.extend(req.replicate_to.iter().cloned());
                let mut seen = HashSet::new();
                targets.retain(|id| seen.insert(id.clone()));
                let min = obj.durability_class.min_valid_copies() as usize;
                if targets.len() < min {
                    return Err(ReclaimError::SurvivabilityViolation(format!(
                        "registration of {:?} object requires at least {min} distinct backend copies, got {}",
                        obj.durability_class,
                        targets.len()
                    )));
                }
                Some((data, hash, target))
            }
            None => {
                if req.target_backend.is_some() || !req.replicate_to.is_empty() {
                    return Err(ReclaimError::InvalidArgument(
                        "target_backend/replicate_to require payload_b64".into(),
                    ));
                }
                let min = obj.durability_class.min_valid_copies();
                if min > 0 {
                    return Err(ReclaimError::SurvivabilityViolation(format!(
                        "registration of {:?} object requires at least {min} valid payload copies",
                        obj.durability_class
                    )));
                }
                None
            }
        };
        if self.with_inner(|g| g.store.get_object(&obj.id))?.is_some() {
            return Err(ReclaimError::InvalidArgument(format!(
                "object {} already exists",
                obj.id
            )));
        }
        obj.lifecycle_state = LifecycleState::Created;
        let now = self.now_ms();
        if obj.created_at_ms == 0 {
            obj.created_at_ms = now;
            obj.last_access_ms = now;
        }
        // Payload storage is prepared before the authoritative object row is
        // published. The per-object guard prevents in-process consumers from
        // racing this temporary replica metadata, and a failed final commit
        // removes every owned replica/dedup reference again.
        let has_payload = prepared_payload.is_some();
        let mut created_replicas = Vec::new();
        if let Some((data, hash, target)) = prepared_payload {
            obj.content_hash = Some(hash);
            obj.physical_size = data.len() as u64;
            created_replicas = self.store_payload(&obj, &target, &data, &req.replicate_to)?;
            obj.replication_count = u32::try_from(created_replicas.len())
                .map_err(|_| ReclaimError::InvalidArgument("replica count exceeds u32".into()))?;
            // Mark the object HOT once it has live physical state.
            obj.lifecycle_state = LifecycleState::Hot;
        }
        let mut audits = vec![AuditEntry {
            id: 0,
            ts_ms: now,
            actor: "coordinator".into(),
            action: "OBJECT_CREATED".into(),
            object_id: Some(obj.id),
            generation: Some(obj.generation),
            prior_state: None,
            // A payload-backed registration persists the final HOT object in
            // one transaction. CREATED remains an audit fact rather than a
            // separately visible intermediate state.
            new_state: (!has_payload).then(|| LifecycleState::Created.as_str().into()),
            policy: None,
            attempt_id: None,
            node: None,
            detail: json!({
                "class": obj.class,
                "logical_size": obj.logical_size,
                "registered_state": LifecycleState::Created.as_str(),
            }),
        }];
        if has_payload {
            audits.push(AuditEntry {
                id: 0,
                ts_ms: now,
                actor: "coordinator".into(),
                action: "OBJECT_PROMOTED_HOT".into(),
                object_id: Some(obj.id),
                generation: Some(obj.generation),
                prior_state: Some(LifecycleState::Created.as_str().into()),
                new_state: Some(LifecycleState::Hot.as_str().into()),
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({}),
            });
        }
        let finalize = self.with_inner(|g| g.store.create_object_with_audits(&obj, &audits));
        if let Err(e) = finalize {
            if !created_replicas.is_empty() {
                self.rollback_replicas(&created_replicas)
                    .map_err(|cleanup| {
                        ReclaimError::Internal(format!(
                        "object registration failed ({e}); replica cleanup also failed ({cleanup})"
                    ))
                    })?;
            }
            return Err(e);
        }
        Ok(obj)
    }

    /// Persist payload bytes to backend(s), deduplicating when the content
    /// already exists. Returns the created replicas.
    fn store_payload(
        &self,
        obj: &ReclaimObject,
        backend_id: &str,
        data: &[u8],
        replicate_to: &[String],
    ) -> Result<Vec<Replica>> {
        let hash = ContentHash::of(data);
        let now = self.now_ms();
        let mut out = Vec::new();

        let mut target_ids = vec![backend_id.to_string()];
        target_ids.extend(replicate_to.iter().cloned());
        let mut seen_targets = HashSet::new();
        target_ids.retain(|id| seen_targets.insert(id.clone()));
        let _payload_operations = self
            .begin_payload_operations(target_ids.iter().map(|id| (id.clone(), hash)).collect())?;
        for (idx, tid) in target_ids.iter().enumerate() {
            match self.store_payload_on_backend(obj, tid, data, hash, now, idx) {
                Ok(replica) => out.push(replica),
                Err(e) => {
                    self.rollback_replicas_under_guard(&out)?;
                    return Err(e);
                }
            }
        }
        Ok(out)
    }

    fn store_payload_on_backend(
        &self,
        obj: &ReclaimObject,
        backend_id: &str,
        data: &[u8],
        hash: ContentHash,
        now: i64,
        replica_index: usize,
    ) -> Result<Replica> {
        let (existing, owner_node) = self.with_inner(|g| {
            Ok((
                g.store.get_dedup(&hash, backend_id)?,
                Self::node_for_backend_locked(g, backend_id)?,
            ))
        })?;
        let (key, stored) = if let Some(entry) = existing {
            if entry.ref_count == 0 {
                return Err(ReclaimError::Dedup(format!(
                    "zero-reference dedup row for {hash} on {backend_id}"
                )));
            }
            let canonical = self.read_payload(backend_id, &entry.key)?;
            crate::integrity::verify_sha256(&canonical, &hash)?;
            self.with_inner(|g| g.store.dedup_acquire(&hash, backend_id))?;
            (entry.key, false)
        } else {
            let key = if replica_index == 0 {
                dedup_key(&hash)
            } else {
                format!("{}-r{replica_index}", dedup_key(&hash))
            };
            (key, true)
        };

        if stored {
            self.write_payload_to(backend_id, &key, data)?;
            if let Err(e) = self.with_inner(|g| {
                g.store.insert_dedup(&DedupEntry {
                    content_hash: hash,
                    backend: backend_id.to_string(),
                    key: key.clone(),
                    ref_count: 1,
                    payload_size: data.len() as u64,
                })?;
                Ok(())
            }) {
                self.delete_payload(backend_id, &key).map_err(|cleanup| {
                    ReclaimError::Internal(format!(
                        "dedup insert failed ({e}); payload cleanup also failed ({cleanup})"
                    ))
                })?;
                return Err(e);
            }
        }

        let replica = Replica {
            replica_id: Uuid::new_v4(),
            object_id: obj.id,
            generation: obj.generation,
            location: crate::object::PhysicalLocation {
                backend: backend_id.to_string(),
                key: key.clone(),
                kind: PhysicalKind::Hot,
            },
            size: data.len() as u64,
            content_hash: hash,
            created_at_ms: now,
            verified_at_ms: Some(now),
            valid: true,
            owner_node,
        };
        if let Err(e) = self.with_inner(|g| {
            g.store.add_replica(&replica)?;
            Ok(())
        }) {
            let last = self.with_inner(|g| g.store.dedup_release(&hash, backend_id))?;
            if last {
                self.delete_payload(backend_id, &key).map_err(|cleanup| {
                    ReclaimError::Internal(format!(
                        "replica insert failed ({e}); payload cleanup also failed ({cleanup})"
                    ))
                })?;
            }
            return Err(e);
        }
        Ok(replica)
    }

    fn rollback_replicas_under_guard(&self, replicas: &[Replica]) -> Result<()> {
        let mut physical = Vec::new();
        self.with_inner(|g| {
            for replica in replicas {
                g.store.delete_replica(&replica.replica_id)?;
                if g.store
                    .dedup_release(&replica.content_hash, &replica.location.backend)?
                {
                    physical.push((
                        replica.location.backend.clone(),
                        replica.location.key.clone(),
                    ));
                }
            }
            Ok(())
        })?;
        for (backend, key) in physical {
            self.delete_payload(&backend, &key)?;
        }
        Ok(())
    }

    fn rollback_replicas(&self, replicas: &[Replica]) -> Result<()> {
        let _payload_operations = self.begin_payload_operations(
            replicas
                .iter()
                .map(|replica| (replica.location.backend.clone(), replica.content_hash))
                .collect(),
        )?;
        self.rollback_replicas_under_guard(replicas)
    }

    fn node_for_backend_locked(g: &CoordinatorInner, backend_id: &str) -> Result<Option<String>> {
        let mut owners: Vec<_> = g
            .nodes
            .values()
            .filter(|node| node.backends.iter().any(|b| b.id == backend_id))
            .map(|node| node.node_id.clone())
            .collect();
        owners.sort();
        if owners.len() > 1 {
            return Err(ReclaimError::ReservationConflict(format!(
                "backend {backend_id} is ambiguously registered by nodes {}",
                owners.join(", ")
            )));
        }
        Ok(owners.pop())
    }

    /// Determine which registered node (if any) hosts a backend id.
    fn node_for_backend(&self, backend_id: &str) -> Result<Option<String>> {
        self.with_inner(|g| Self::node_for_backend_locked(g, backend_id))
    }

    /// Write a payload to a backend, local or node-hosted.
    fn write_payload_to(&self, backend_id: &str, key: &str, data: &[u8]) -> Result<()> {
        let expected = ContentHash::of(data);
        let node_id = self.node_for_backend(backend_id)?;
        let write_result = match node_id {
            None => {
                let backend = self.with_inner(|g| g.backends.get(backend_id))?;
                let size = backend.put(key, data)?;
                if size != data.len() as u64 {
                    Err(ReclaimError::Backend(format!(
                        "backend {backend_id} reported storing {size} bytes, expected {}",
                        data.len()
                    )))
                } else {
                    let stored = backend.get(key)?;
                    crate::integrity::verify_sha256(&stored, &expected)
                }
            }
            Some(nid) => {
                let reply = self.node_operation(
                    &nid,
                    crate::protocol::method::NODE_EXECUTE_STORE,
                    NodeOperationRequest {
                        object_id: Uuid::nil(),
                        generation: 0,
                        replica_id: Uuid::nil(),
                        attempt_id: None,
                        coordinator_epoch: self.epoch(),
                        backend: backend_id.to_string(),
                        key: key.to_string(),
                        payload_b64: Some(encode_base64(data)),
                        expected_hash: Some(expected.to_string()),
                        codec: None,
                    },
                )?;
                if !reply.ok {
                    Err(reply
                        .error
                        .map(ReclaimError::from)
                        .unwrap_or_else(|| ReclaimError::Backend("node store failed".into())))
                } else if reply.size != Some(data.len() as u64) {
                    Err(ReclaimError::Protocol(format!(
                        "node store returned size {:?}, expected {}",
                        reply.size,
                        data.len()
                    )))
                } else if reply.result_hash.as_deref() != Some(encode_base64(&expected.0).as_str())
                {
                    Err(ReclaimError::Protocol(
                        "node store returned a mismatched content hash".into(),
                    ))
                } else {
                    Ok(())
                }
            }
        };
        if let Err(e) = write_result {
            return match self.delete_payload(backend_id, key) {
                Ok(()) => Err(e),
                Err(cleanup) => Err(ReclaimError::Internal(format!(
                    "payload write verification failed ({e}); cleanup also failed ({cleanup})"
                ))),
            };
        }
        Ok(())
    }

    /// Read a payload from a backend, local or node-hosted.
    pub fn read_payload(&self, backend_id: &str, key: &str) -> Result<Vec<u8>> {
        let node_id = self.node_for_backend(backend_id)?;
        match node_id {
            None => {
                let backend = self.with_inner(|g| g.backends.get(backend_id))?;
                backend.get(key)
            }
            Some(nid) => {
                let reply = self.node_operation(
                    &nid,
                    crate::protocol::method::NODE_EXECUTE_READ,
                    NodeOperationRequest {
                        object_id: Uuid::nil(),
                        generation: 0,
                        replica_id: Uuid::nil(),
                        attempt_id: None,
                        coordinator_epoch: self.epoch(),
                        backend: backend_id.to_string(),
                        key: key.to_string(),
                        payload_b64: None,
                        expected_hash: None,
                        codec: None,
                    },
                )?;
                if !reply.ok {
                    return Err(reply
                        .error
                        .map(ReclaimError::from)
                        .unwrap_or_else(|| ReclaimError::Backend("node read failed".into())));
                }
                let b64 = reply.result_hash.as_deref().ok_or_else(|| {
                    ReclaimError::Protocol("node read returned no payload".into())
                })?;
                decode_base64(b64)
            }
        }
    }

    /// Delete a payload from a backend, local or node-hosted.
    fn delete_payload(&self, backend_id: &str, key: &str) -> Result<()> {
        let node_id = self.node_for_backend(backend_id)?;
        match node_id {
            None => {
                let backend = self.with_inner(|g| g.backends.get(backend_id))?;
                backend.delete(key)?;
            }
            Some(nid) => {
                let reply = self.node_operation(
                    &nid,
                    crate::protocol::method::NODE_EXECUTE_DELETE,
                    NodeOperationRequest {
                        object_id: Uuid::nil(),
                        generation: 0,
                        replica_id: Uuid::nil(),
                        attempt_id: None,
                        coordinator_epoch: self.epoch(),
                        backend: backend_id.to_string(),
                        key: key.to_string(),
                        payload_b64: None,
                        expected_hash: None,
                        codec: None,
                    },
                )?;
                if !reply.ok {
                    return Err(reply
                        .error
                        .map(ReclaimError::from)
                        .unwrap_or_else(|| ReclaimError::Backend("node delete failed".into())));
                }
            }
        }
        Ok(())
    }

    /// Does a payload exist on a backend (local or node-hosted)?
    fn payload_exists(&self, backend_id: &str, key: &str) -> Result<bool> {
        let node_id = self.node_for_backend(backend_id)?;
        match node_id {
            None => {
                let backend = self.with_inner(|g| g.backends.get(backend_id))?;
                backend.exists(key)
            }
            Some(nid) => {
                let reply = self.node_operation(
                    &nid,
                    crate::protocol::method::NODE_EXECUTE_EXISTS,
                    NodeOperationRequest {
                        object_id: Uuid::nil(),
                        generation: 0,
                        replica_id: Uuid::nil(),
                        attempt_id: None,
                        coordinator_epoch: self.epoch(),
                        backend: backend_id.to_string(),
                        key: key.to_string(),
                        payload_b64: None,
                        expected_hash: None,
                        codec: None,
                    },
                )?;
                if !reply.ok {
                    return Err(reply
                        .error
                        .map(ReclaimError::from)
                        .unwrap_or_else(|| ReclaimError::Backend("node exists failed".into())));
                }
                Ok(reply.existed)
            }
        }
    }

    /// Send an operation to a node and await its reply. Fresh connection per
    /// call (short-lived connections; no shared mutable client state).
    pub fn node_operation(
        &self,
        node_id: &str,
        method: &str,
        mut op: NodeOperationRequest,
    ) -> Result<NodeOperationReply> {
        let addr = self.with_inner(|guard| {
            guard
                .nodes
                .get(node_id)
                .map(|n| n.addr.clone())
                .ok_or_else(|| ReclaimError::NotFound(format!("node {node_id}")))
        })?;
        // Authority is coordinator-owned state, never caller-controlled.
        op.coordinator_epoch = self.epoch;
        let mut client = Client::connect(&addr, 15_000)?;
        let payload = serde_json::to_value(op)?;
        let reply = client.call(method, payload)?;
        let id = reply.id;
        let value = reply.into_result(id)?;
        Ok(serde_json::from_value(value)?)
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    /// Touch an object (access tracking).
    pub fn touch(&self, id: &Uuid, actor: &str) -> Result<ReclaimObject> {
        let _operation = self.begin_object_operation(id)?;
        let mut obj = self.with_inner(|g| g.store.require_object(id))?;
        if obj.lifecycle_state == LifecycleState::Reclaimed {
            return Err(ReclaimError::InvalidArgument(
                "cannot touch a RECLAIMED object".into(),
            ));
        }
        obj.access_count = obj.access_count.checked_add(1).ok_or_else(|| {
            ReclaimError::InvalidArgument(format!("object {} access count overflow", obj.id))
        })?;
        obj.last_access_ms = self.now_ms();
        self.apply_object_update(&obj, actor, None, "OBJECT_TOUCHED")?;
        Ok(obj)
    }

    pub fn pin(&self, id: &Uuid, actor: &str) -> Result<ReclaimObject> {
        let _operation = self.begin_object_operation(id)?;
        let mut obj = self.with_inner(|g| g.store.require_object(id))?;
        if obj.lifecycle_state == LifecycleState::Reclaimed {
            return Err(ReclaimError::InvalidArgument(
                "cannot pin a RECLAIMED object".into(),
            ));
        }
        obj.pinned = true;
        self.apply_object_update(&obj, actor, None, "OBJECT_PINNED")?;
        Ok(obj)
    }

    pub fn unpin(&self, id: &Uuid, actor: &str) -> Result<ReclaimObject> {
        let _operation = self.begin_object_operation(id)?;
        let mut obj = self.with_inner(|g| g.store.require_object(id))?;
        obj.pinned = false;
        self.apply_object_update(&obj, actor, None, "OBJECT_UNPINNED")?;
        Ok(obj)
    }

    pub fn set_protected(&self, id: &Uuid, protected: bool, actor: &str) -> Result<ReclaimObject> {
        let _operation = self.begin_object_operation(id)?;
        let mut obj = self.with_inner(|g| g.store.require_object(id))?;
        obj.protected = protected;
        self.apply_object_update(
            &obj,
            actor,
            None,
            if protected {
                "OBJECT_PROTECTED"
            } else {
                "OBJECT_UNPROTECTED"
            },
        )?;
        Ok(obj)
    }

    /// Apply a state transition with validation, persistence, and audit.
    fn apply_transition(
        &self,
        obj: &ReclaimObject,
        to: LifecycleState,
        actor: &str,
        action: &str,
        policy: Option<&str>,
        attempt: Option<Uuid>,
    ) -> Result<ReclaimObject> {
        let result = crate::lifecycle::check_transition(obj.lifecycle_state, to)?;
        let mut updated = obj.clone();
        if result == TransitionResult::Noop {
            return Ok(updated);
        }
        let from = obj.lifecycle_state;
        updated.lifecycle_state = to;
        self.with_inner(|g| {
            g.store.update_object_with_audit(
                &updated,
                &AuditEntry {
                    id: 0,
                    ts_ms: self.clock.now_ms(),
                    actor: actor.into(),
                    action: action.into(),
                    object_id: Some(obj.id),
                    generation: Some(obj.generation),
                    prior_state: Some(from.as_str().into()),
                    new_state: Some(to.as_str().into()),
                    policy: policy.map(|s| s.to_string()),
                    attempt_id: attempt,
                    node: None,
                    detail: json!({}),
                },
            )?;
            Ok(())
        })?;
        Ok(updated)
    }

    /// Persist an object update + audit row.
    fn apply_object_update(
        &self,
        obj: &ReclaimObject,
        actor: &str,
        attempt: Option<Uuid>,
        action: &str,
    ) -> Result<()> {
        self.with_inner(|g| {
            g.store.update_object_with_audit(
                obj,
                &AuditEntry {
                    id: 0,
                    ts_ms: self.clock.now_ms(),
                    actor: actor.into(),
                    action: action.into(),
                    object_id: Some(obj.id),
                    generation: Some(obj.generation),
                    prior_state: None,
                    new_state: Some(obj.lifecycle_state.as_str().into()),
                    policy: None,
                    attempt_id: attempt,
                    node: None,
                    detail: json!({}),
                },
            )?;
            Ok(())
        })
    }

    // ------------------------------------------------------------------
    // Lineage
    // ------------------------------------------------------------------

    pub fn add_lineage(
        &self,
        parent: Uuid,
        child: Uuid,
        kind: EdgeKind,
        actor: &str,
    ) -> Result<()> {
        self.with_inner(|g| {
            if g.active_recovery
                || g.active_objects.contains(&parent)
                || g.active_objects.contains(&child)
            {
                return Err(ReclaimError::ReservationConflict(
                    "lineage endpoint has an active lifecycle operation".into(),
                ));
            }
            // Validate and insert under the same coordinator lock. Otherwise
            // two concurrent individually-acyclic additions can jointly
            // create a cycle (A->B racing B->A).
            let mut graph = g.store.lineage_graph()?;
            graph.add_edge(parent, child, kind)?;
            g.store.add_lineage_edge(parent, child, kind)?;
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: self.clock.now_ms(),
                actor: actor.into(),
                action: format!("LINEAGE_ADD_{}", kind.as_str()),
                object_id: Some(parent),
                generation: None,
                prior_state: None,
                new_state: None,
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({"parent": parent.to_string(), "child": child.to_string()}),
            })?;
            Ok(())
        })
    }

    pub fn remove_lineage(
        &self,
        parent: Uuid,
        child: Uuid,
        kind: EdgeKind,
        actor: &str,
    ) -> Result<()> {
        self.with_inner(|g| {
            if g.active_recovery
                || g.active_objects.contains(&parent)
                || g.active_objects.contains(&child)
            {
                return Err(ReclaimError::ReservationConflict(
                    "lineage endpoint has an active lifecycle operation".into(),
                ));
            }
            g.store.remove_lineage_edge(parent, child, kind)?;
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: self.clock.now_ms(),
                actor: actor.into(),
                action: format!("LINEAGE_REMOVE_{}", kind.as_str()),
                object_id: Some(parent),
                generation: None,
                prior_state: None,
                new_state: None,
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({"parent": parent.to_string(), "child": child.to_string()}),
            })?;
            Ok(())
        })
    }

    pub fn lineage(&self) -> Result<LineageGraph> {
        self.with_inner(|g| g.store.lineage_graph())
    }

    // ------------------------------------------------------------------
    // Planning / decisions
    // ------------------------------------------------------------------

    /// Reconstructibility predicate used for dependency safety. An object is
    /// reconstructible when it has an explicit recomputation recipe (cost).
    fn reconstructible(obj: &ReclaimObject) -> bool {
        obj.recompute_cost.is_some()
    }

    /// Compute lineage-derived values for the decision context.
    fn decision_context(&self, obj: &ReclaimObject) -> Result<DecisionContext> {
        let graph = self.lineage()?;
        let all = self.with_inner(|g| g.store.list_objects())?;
        let reconstructible_map: HashMap<Uuid, bool> = all
            .iter()
            .map(|o| (o.id, Self::reconstructible(o)))
            .collect();
        let dead_nodes: HashSet<Uuid> = all
            .iter()
            .filter(|o| o.lifecycle_state == LifecycleState::Reclaimed)
            .map(|o| o.id)
            .collect();
        // Dependency value: each non-reconstructible dependent that depends on
        // this object adds retention value.
        let mut dependency_value = 0.0;
        if let Some(edges) = graph.parents.get(&obj.id) {
            for e in edges {
                if e.kind == EdgeKind::DependsOn
                    && !dead_nodes.contains(&e.child)
                    && !reconstructible_map.get(&e.child).copied().unwrap_or(false)
                {
                    dependency_value += 1.0;
                }
            }
        }
        let valid_copies = self.with_inner(|g| g.store.valid_replica_count(&obj.id))?
            + self.with_inner(|g| g.store.archives_for(&obj.id))?.len() as u64;
        let survivability_value = if valid_copies > 0 {
            (obj.durability_class.min_valid_copies() as f64).max(1.0)
        } else {
            0.0
        };
        Ok(DecisionContext {
            dependency_value: dependency_value * 10_000.0,
            survivability_value,
        })
    }

    /// Produce (and persist) a deterministic decision for one object.
    pub fn plan(&self, id: &Uuid, actor: &str) -> Result<PolicyDecision> {
        let obj = self.with_inner(|g| g.store.require_object(id))?;
        let context = self.decision_context(&obj)?;
        let pressure = self.with_inner(|g| g.pressure.try_level())?;
        let epoch = self.epoch();
        let now = self.now_ms();
        let registry = self.with_inner(|g| Ok(g.policies.clone()))?;
        let decision = crate::policy::decide(&registry, &obj, pressure, epoch, now, &context)?;
        self.with_inner(|g| {
            g.store.insert_decision(&decision.decision)?;
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: now,
                actor: actor.into(),
                action: "DECISION".into(),
                object_id: Some(obj.id),
                generation: Some(obj.generation),
                prior_state: Some(obj.lifecycle_state.as_str().into()),
                new_state: None,
                policy: Some(decision.decision.policy_id.clone()),
                attempt_id: None,
                node: None,
                detail: json!({
                    "verdict": format!("{:?}", decision.decision.verdict),
                    "score": decision.decision.score,
                    "threshold": decision.decision.threshold,
                    "explanation": decision.explanation,
                }),
            })?;
            Ok(())
        })?;
        Ok(decision)
    }

    /// List reclaim candidates deterministically (score desc, id asc).
    pub fn candidates(&self, limit: u64, actor: &str) -> Result<Vec<PolicyDecision>> {
        const MAX_CANDIDATES: u64 = 10_000;
        if limit > MAX_CANDIDATES {
            return Err(ReclaimError::InvalidArgument(format!(
                "candidate limit {limit} exceeds maximum {MAX_CANDIDATES}"
            )));
        }
        // Batch plan: load everything once, decide in memory (O(n), not O(n²)
        // — per-object store re-scans made 100K-object planning quadratic).
        let (objects, graph, replica_counts, archive_counts, pressure, registry) = self
            .with_inner(|g| {
                Ok((
                    g.store.list_objects()?,
                    g.store.lineage_graph()?,
                    g.store.valid_replica_counts()?,
                    g.store.archive_counts()?,
                    g.pressure.try_level()?,
                    g.policies.clone(),
                ))
            })?;
        let reconstructible_map: HashMap<Uuid, bool> = objects
            .iter()
            .map(|o| (o.id, Self::reconstructible(o)))
            .collect();
        let dead_nodes: HashSet<Uuid> = objects
            .iter()
            .filter(|o| o.lifecycle_state == LifecycleState::Reclaimed)
            .map(|o| o.id)
            .collect();
        let epoch = self.epoch();
        let now = self.now_ms();
        let mut results: Vec<PolicyDecision> = Vec::new();
        for obj in objects {
            if obj.lifecycle_state == LifecycleState::Reclaimed
                || obj.lifecycle_state == LifecycleState::Failed
            {
                continue;
            }
            // Lineage-derived dependency value (single graph query, batched).
            let mut dependency_value = 0.0;
            if let Some(edges) = graph.parents.get(&obj.id) {
                for e in edges {
                    if e.kind == EdgeKind::DependsOn
                        && !dead_nodes.contains(&e.child)
                        && !reconstructible_map.get(&e.child).copied().unwrap_or(false)
                    {
                        dependency_value += 1.0;
                    }
                }
            }
            let valid_copies = replica_counts.get(&obj.id).copied().unwrap_or(0)
                + archive_counts.get(&obj.id).copied().unwrap_or(0);
            let survivability_value = if valid_copies > 0 {
                (obj.durability_class.min_valid_copies() as f64).max(1.0)
            } else {
                0.0
            };
            let context = DecisionContext {
                dependency_value: dependency_value * 10_000.0,
                survivability_value,
            };
            let decision = crate::policy::decide(&registry, &obj, pressure, epoch, now, &context)?;
            if decision.decision.verdict == crate::economics::ReclaimVerdict::Reclaim {
                results.push(decision);
            }
        }
        results.sort_by(|a, b| {
            b.decision
                .score
                .partial_cmp(&a.decision.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.decision.object_id.cmp(&b.decision.object_id))
        });
        results.truncate(usize::try_from(limit).map_err(|_| {
            ReclaimError::InvalidArgument("candidate limit does not fit usize".into())
        })?);
        for d in &results {
            self.with_inner(|g| {
                g.store.insert_decision(&d.decision)?;
                Ok(())
            })?;
        }
        let _ = actor;
        Ok(results)
    }

    // ------------------------------------------------------------------
    // Reclaim transaction
    // ------------------------------------------------------------------

    /// Number of valid copies counting valid replicas and archives.
    fn valid_copy_count(&self, obj_id: &Uuid) -> Result<u64> {
        Ok(self.with_inner(|g| g.store.valid_replica_count(obj_id))?
            + self.with_inner(|g| g.store.archives_for(obj_id))?.len() as u64)
    }

    /// Survivability check: after deleting `to_delete` replicas and
    /// `to_delete_archives`, the object must still hold at least
    /// `durability_class.min_valid_copies()` valid copies.
    fn check_survivability(
        &self,
        obj: &ReclaimObject,
        to_delete: &[Replica],
        to_delete_archives: usize,
    ) -> Result<()> {
        let valid_before = self.valid_copy_count(&obj.id)?;
        let deleted_valid = to_delete.iter().filter(|r| r.valid).count() as u64;
        let remaining = valid_before
            .saturating_sub(deleted_valid)
            .saturating_sub(to_delete_archives as u64);
        let min = obj.durability_class.min_valid_copies();
        if remaining < min as u64 {
            return Err(ReclaimError::SurvivabilityViolation(format!(
                "after reclaim: {remaining} valid copies remain, minimum for {:?} is {min}",
                obj.durability_class
            )));
        }
        Ok(())
    }

    /// Dependency check: reclamation must not invalidate a non-reconstructible
    /// dependent.
    fn check_dependencies(&self, obj: &ReclaimObject) -> Result<()> {
        let graph = self.lineage()?;
        let all = self.with_inner(|g| g.store.list_objects())?;
        let reconstructible_map: HashMap<Uuid, bool> = all
            .iter()
            .map(|o| (o.id, Self::reconstructible(o)))
            .collect();
        let dead_nodes: HashSet<Uuid> = all
            .iter()
            .filter(|o| o.lifecycle_state == LifecycleState::Reclaimed)
            .map(|o| o.id)
            .collect();
        graph.dependency_safe(
            obj.id,
            &|id| reconstructible_map.get(&id).copied().unwrap_or(false),
            &dead_nodes,
        )
    }

    /// Run the full transactional reclaim lifecycle for one object.
    ///
    /// plan -> reserve -> validate -> execute -> verify -> commit
    pub fn reclaim(&self, req: &ReclaimRequest) -> Result<ReclaimReport> {
        let _operation = self.begin_object_operation(&req.object_id)?;
        let now = self.now_ms();
        let obj = self.with_inner(|g| g.store.require_object(&req.object_id))?;

        // Pre-checks: hard invariants fail immediately and deterministically.
        if obj.pinned {
            return Err(ReclaimError::PinnedObject(obj.id.to_string()));
        }
        if obj.protected {
            return Err(ReclaimError::ProtectedObject(obj.id.to_string()));
        }
        if obj.lifecycle_state == LifecycleState::Reclaimed {
            return Err(ReclaimError::InvalidArgument(format!(
                "object {} is already RECLAIMED",
                obj.id
            )));
        }
        if obj.lifecycle_state == LifecycleState::Failed {
            return Err(ReclaimError::InvalidArgument(format!(
                "object {} is FAILED and requires operator repair",
                obj.id
            )));
        }
        if self.with_inner(|g| g.store.has_open_reservation_for(&obj.id))? {
            return Err(ReclaimError::ReservationConflict(format!(
                "object {} already has an open reclamation reservation",
                obj.id
            )));
        }
        if let Some(deadline) = obj.min_retention_deadline_ms {
            if now < deadline {
                return Err(ReclaimError::InvalidArgument(format!(
                    "object {} is before its minimum retention deadline",
                    obj.id
                )));
            }
        }

        // 1. PLAN: deterministic decision. Without a reclaim verdict (and
        // without force), we stop here — nothing is reserved.
        let decision = self.plan(&obj.id, &req.actor)?;
        let mut report = ReclaimReport {
            object_id: obj.id,
            generation: obj.generation,
            reclaimed: false,
            prior_state: obj.lifecycle_state.as_str().into(),
            final_state: obj.lifecycle_state.as_str().into(),
            attempt_id: Uuid::new_v4(),
            reservation_id: Uuid::new_v4(),
            decision: Some(decision.clone()),
            explanation: Some(decision.explanation.clone()),
            deleted_replicas: Vec::new(),
            deleted_archives: Vec::new(),
            physical_error: None,
        };
        if decision.decision.verdict != crate::economics::ReclaimVerdict::Reclaim && !req.force {
            return Ok(report);
        }

        // Deletion set: reclaim releases every valid physical copy. The
        // survivability guard below ensures the object never falls below its
        // durability class's minimum valid copy count; DURABLE/CRITICAL
        // objects are therefore not automatically reclaimable while their
        // protected copies exist (that is the invariant, not a bug).
        let replicas = self.with_inner(|g| g.store.replicas_for(&obj.id))?;
        let archives = self.with_inner(|g| g.store.archives_for(&obj.id))?;
        // Reclamation releases every owned replica. Invalid replicas do not
        // count toward survivability, but retaining their rows/refcounts would
        // leak corrupt physical state after the object becomes RECLAIMED.
        let to_delete: Vec<Replica> = replicas.clone();
        let to_delete_archives = archives.clone();

        // Survivability must hold BEFORE we reserve anything.
        self.check_survivability(&obj, &to_delete, to_delete_archives.len())?;

        // Preflight the complete metadata route before creating any durable
        // attempt/reservation/journal rows. Every subsequent transition is
        // therefore known legal unless authoritative state is corrupted.
        if matches!(
            obj.lifecycle_state,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Cold
                | LifecycleState::Compressed
        ) {
            if !Self::reconstructible(&obj) && obj.durability_class != DurabilityClass::Ephemeral {
                return Err(ReclaimError::SurvivabilityViolation(format!(
                    "object {} is not reconstructible and may not be reclaimed",
                    obj.id
                )));
            }
            crate::lifecycle::check_transition(obj.lifecycle_state, LifecycleState::Recomputable)?;
            crate::lifecycle::check_transition(
                LifecycleState::Recomputable,
                LifecycleState::ReclaimPending,
            )?;
        } else {
            crate::lifecycle::check_transition(
                obj.lifecycle_state,
                LifecycleState::ReclaimPending,
            )?;
        }

        // 2. RESERVE: attempt + reservation + journal + state transition.
        let prior_obj = obj.clone();
        let journal_payload = journal_payload(&prior_obj, &to_delete, &to_delete_archives)?;
        let attempt = Attempt {
            attempt_id: report.attempt_id,
            object_id: obj.id,
            generation: obj.generation,
            node: "coordinator".into(),
            created_at_ms: now,
            updated_at_ms: now,
            status: AttemptStatus::Open,
        };
        let reservation_ttl = self.with_inner(|g| Ok(g.config.reservation_ttl_ms))?;
        let expires_at_ms = now.checked_add(reservation_ttl).ok_or_else(|| {
            ReclaimError::InvalidArgument("reservation expiration timestamp overflow".into())
        })?;
        let reservation = Reservation {
            reservation_id: report.reservation_id,
            attempt_id: report.attempt_id,
            object_id: obj.id,
            generation: obj.generation,
            node: "coordinator".into(),
            created_at_ms: now,
            expires_at_ms,
            status: "OPEN".into(),
        };
        self.with_inner(|g| {
            g.store.create_reclaim_reservation(
                &JournalEntry {
                    attempt_id: report.attempt_id,
                    object_id: obj.id,
                    generation: obj.generation,
                    phase: JournalPhase::Reserved,
                    created_at_ms: now,
                    updated_at_ms: now,
                    payload: journal_payload.clone(),
                },
                &reservation,
                &attempt,
            )
        })?;

        // Move the object toward RECLAIM_PENDING through legal transitions.
        let mut current = prior_obj.clone();
        if current.lifecycle_state != LifecycleState::ReclaimPending {
            if matches!(
                current.lifecycle_state,
                LifecycleState::Hot
                    | LifecycleState::Warm
                    | LifecycleState::Cold
                    | LifecycleState::Compressed
            ) {
                current = self.apply_transition(
                    &current,
                    LifecycleState::Recomputable,
                    &req.actor,
                    "RECLAIM_STEP_RECOMPUTABLE",
                    Some(&decision.decision.policy_id),
                    Some(report.attempt_id),
                )?;
            }
            current = self.apply_transition(
                &current,
                LifecycleState::ReclaimPending,
                &req.actor,
                "RECLAIM_RESERVED",
                Some(&decision.decision.policy_id),
                Some(report.attempt_id),
            )?;
        }
        report.final_state = LifecycleState::ReclaimPending.as_str().into();

        // 3. VALIDATE: revalidate dependencies and survivability after the
        // reservation, before any physical action.
        self.with_inner(|g| {
            g.store.update_journal_phase(
                &report.attempt_id,
                JournalPhase::Validated,
                self.clock.now_ms(),
            )
        })?;
        if let Err(e) = self.check_survivability(&prior_obj, &to_delete, to_delete_archives.len()) {
            self.abort_reclaim(&report, &prior_obj, &e)?;
            return Err(e);
        }
        if let Err(e) = self.check_dependencies(&prior_obj) {
            self.abort_reclaim(&report, &prior_obj, &e)?;
            return Err(e);
        }

        // 4. EXECUTE: release dedup references and decide what must be
        // physically deleted, atomically under the coordinator lock. A shared
        // dedup payload is only deleted when the LAST live reference is
        // released (invariant: shared physical content survives while any
        // live reference remains).
        let _payload_operations = match self.begin_payload_operations(
            to_delete
                .iter()
                .map(|replica| (replica.location.backend.clone(), replica.content_hash))
                .collect(),
        ) {
            Ok(guard) => guard,
            Err(e) => {
                self.abort_reclaim(&report, &prior_obj, &e)?;
                return Err(e);
            }
        };
        let physical_plan: Vec<Replica> = self.with_inner(|g| {
            let mut scheduled: HashMap<(ContentHash, String), (usize, Replica)> = HashMap::new();
            let mut physical = Vec::new();
            for replica in &to_delete {
                if replica.content_hash == ContentHash::empty() {
                    physical.push(replica.clone());
                    continue;
                }
                let key = (replica.content_hash, replica.location.backend.clone());
                let entry = scheduled
                    .entry(key)
                    .or_insert_with(|| (0, replica.clone()));
                entry.0 += 1;
                if entry.1.location.key != replica.location.key {
                    return Err(ReclaimError::Dedup(format!(
                        "replicas for {} on {} disagree on canonical key",
                        replica.content_hash, replica.location.backend
                    )));
                }
            }
            for ((hash, backend), (scheduled_count, representative)) in scheduled {
                let entry = g.store.get_dedup(&hash, &backend)?.ok_or_else(|| {
                    ReclaimError::Dedup(format!(
                        "missing dedup row for scheduled replica {hash} on {backend}"
                    ))
                })?;
                if entry.key != representative.location.key {
                    return Err(ReclaimError::Dedup(format!(
                        "dedup key {} disagrees with replica key {} for {hash} on {backend}",
                        entry.key, representative.location.key
                    )));
                }
                let scheduled_count = u64::try_from(scheduled_count).map_err(|_| {
                    ReclaimError::Dedup("scheduled replica count exceeds u64".into())
                })?;
                if scheduled_count > entry.ref_count {
                    return Err(ReclaimError::Dedup(format!(
                        "scheduled {scheduled_count} releases but dedup refcount is {} for {hash} on {backend}",
                        entry.ref_count
                    )));
                }
                if scheduled_count == entry.ref_count {
                    physical.push(representative);
                }
            }
            Ok(physical)
        })?;
        let planned_payload =
            crate::recovery::with_physical_deletions(&journal_payload, &physical_plan)?;
        if let Err(e) = self.with_inner(|g| {
            g.store.start_journal_physical(
                &report.attempt_id,
                &planned_payload,
                self.clock.now_ms(),
            )
        }) {
            self.abort_reclaim(&report, &prior_obj, &e)?;
            return Err(e);
        }
        let mut deleted: Vec<Uuid> = Vec::new();
        let mut deleted_archives: Vec<String> = Vec::new();
        for replica in &physical_plan {
            let backend_id = &replica.location.backend;
            let key = &replica.location.key;
            match self.delete_payload(backend_id, key) {
                Ok(()) => {
                    deleted.push(replica.replica_id);
                }
                Err(e) => {
                    return Err(self.indeterminate_reclaim(&report, &e, "PHYSICAL_RECLAIM_FAILED"));
                }
            }
        }
        for archive in &to_delete_archives {
            let backend = {
                let guard = self
                    .inner
                    .lock()
                    .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
                guard
                    .archives
                    .iter()
                    .find(|a| a.id() == archive.backend)
                    .cloned()
                    .ok_or_else(|| {
                        ReclaimError::ArchiveFailure(format!(
                            "archive backend {} unavailable",
                            archive.backend
                        ))
                    })?
            };
            let archive_result = backend.delete(&archive.key);
            match archive_result {
                Ok(()) => deleted_archives.push(archive.archive_id.clone()),
                Err(e) => {
                    return Err(self.indeterminate_reclaim(&report, &e, "ARCHIVE_RECLAIM_FAILED"));
                }
            }
        }

        // 5. VERIFY: confirm the physical state is gone for the payloads we
        // were entitled to delete (shared dedup payloads survive elsewhere).
        for replica in &physical_plan {
            let backend_id = &replica.location.backend;
            let key = &replica.location.key;
            let exists = self
                .payload_exists(backend_id, key)
                .map_err(|e| self.indeterminate_reclaim(&report, &e, "RECLAIM_VERIFY_FAILED"))?;
            if exists {
                let e = ReclaimError::IntegrityFailure(format!(
                    "payload still exists after reclaim: {backend_id}:{key}"
                ));
                return Err(self.indeterminate_reclaim(&report, &e, "RECLAIM_VERIFY_FAILED"));
            }
        }
        if let Err(e) = self.with_inner(|g| {
            g.store.update_journal_phase(
                &report.attempt_id,
                JournalPhase::PhysicalDone,
                self.clock.now_ms(),
            )
        }) {
            return Err(self.indeterminate_reclaim(&report, &e, "PHYSICAL_DONE_PERSIST_FAILED"));
        }

        // 6. COMMIT: now that physical deletion is durable and verified,
        // release ownership metadata. If any write fails, PHYSICAL_DONE stays
        // open so startup recovery can deterministically finish the commit.
        let commit_result = self.with_inner(|g| {
            for replica in &to_delete {
                g.store.delete_replica(&replica.replica_id)?;
                if replica.content_hash != ContentHash::empty() {
                    g.store
                        .dedup_release(&replica.content_hash, &replica.location.backend)?;
                }
            }
            for archive in &to_delete_archives {
                g.store.delete_archive(&archive.archive_id)?;
            }
            let mut reclaimed = current.clone();
            reclaimed.lifecycle_state = LifecycleState::Reclaimed;
            g.store.update_object_with_audit(&reclaimed, &AuditEntry {
                id: 0,
                ts_ms: self.clock.now_ms(),
                actor: req.actor.clone(),
                action: "RECLAIM_COMMITTED".into(),
                object_id: Some(obj.id),
                generation: Some(obj.generation),
                prior_state: Some(LifecycleState::ReclaimPending.as_str().into()),
                new_state: Some(LifecycleState::Reclaimed.as_str().into()),
                policy: Some(decision.decision.policy_id.clone()),
                attempt_id: Some(report.attempt_id),
                node: None,
                detail: json!({
                    "deleted_replicas": deleted.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
                    "deleted_archives": deleted_archives,
                }),
            })?;
            g.store
                .update_reservation(&report.reservation_id, "COMMITTED")?;
            g.store.update_attempt(
                &report.attempt_id,
                AttemptStatus::Committed,
                self.clock.now_ms(),
            )?;
            g.store.update_journal_phase(
                &report.attempt_id,
                JournalPhase::Committed,
                self.clock.now_ms(),
            )?;
            Ok(())
        });
        if let Err(e) = commit_result {
            return Err(self.indeterminate_reclaim(&report, &e, "RECLAIM_COMMIT_FAILED"));
        }

        report.reclaimed = true;
        report.final_state = LifecycleState::Reclaimed.as_str().into();
        report.deleted_replicas = deleted;
        report.deleted_archives = deleted_archives;
        Ok(report)
    }

    /// Preserve an open physical-phase journal and stop accepting work when
    /// backend truth is no longer safely reversible in-process. Recovery must
    /// classify and finish this attempt before a coordinator may restart.
    fn indeterminate_reclaim(
        &self,
        report: &ReclaimReport,
        reason: &ReclaimError,
        kind: &str,
    ) -> ReclaimError {
        let message = format!(
            "reclaim {} became indeterminate during {kind}: {reason}; coordinator is shutting down for recovery",
            report.attempt_id
        );
        let failure_result = self.with_inner(|g| {
            g.store.insert_failure(&FailureRecord {
                id: 0,
                ts_ms: self.clock.now_ms(),
                object_id: Some(report.object_id),
                attempt_id: Some(report.attempt_id),
                kind: kind.into(),
                message: reason.to_string(),
                recovered: false,
            })
        });
        self.authority_lost
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.request_shutdown();
        match failure_result {
            Ok(()) => ReclaimError::Recovery(message),
            Err(failure_error) => ReclaimError::Recovery(format!(
                "{message}; recording the failure also failed: {failure_error}"
            )),
        }
    }

    /// Abort a reclaim: roll metadata back to the prior state, close the
    /// reservation/attempt, and record the reason.
    fn abort_reclaim(
        &self,
        report: &ReclaimReport,
        prior_obj: &ReclaimObject,
        reason: &ReclaimError,
    ) -> Result<()> {
        let now = self.now_ms();
        self.with_inner(|g| {
            g.store.update_object_with_audit(
                prior_obj,
                &AuditEntry {
                    id: 0,
                    ts_ms: now,
                    actor: "coordinator".into(),
                    action: "RECLAIM_ABORTED".into(),
                    object_id: Some(prior_obj.id),
                    generation: Some(prior_obj.generation),
                    prior_state: Some(LifecycleState::ReclaimPending.as_str().into()),
                    new_state: Some(prior_obj.lifecycle_state.as_str().into()),
                    policy: None,
                    attempt_id: Some(report.attempt_id),
                    node: None,
                    detail: json!({"reason": reason.to_string()}),
                },
            )?;
            g.store
                .update_reservation(&report.reservation_id, "RELEASED")?;
            g.store
                .update_attempt(&report.attempt_id, AttemptStatus::RolledBack, now)?;
            g.store
                .update_journal_phase(&report.attempt_id, JournalPhase::RolledBack, now)
        })
    }

    // ------------------------------------------------------------------
    // Compress / archive / restore / verify
    // ------------------------------------------------------------------

    /// Compress the primary payload and record compressed accounting.
    pub fn compress(
        &self,
        id: &Uuid,
        actor: &str,
    ) -> Result<crate::compression::CompressionResult> {
        let _operation = self.begin_object_operation(id)?;
        let obj = self.with_inner(|g| g.store.require_object(id))?;
        if obj.lifecycle_state == LifecycleState::Compressed {
            return Err(ReclaimError::InvalidArgument(
                "object is already COMPRESSED".into(),
            ));
        }
        if obj.app_metadata.contains_key(COMPRESSION_ORIGINAL_SIZE_KEY) {
            return Err(ReclaimError::InvalidArgument(format!(
                "application metadata key {COMPRESSION_ORIGINAL_SIZE_KEY:?} is reserved"
            )));
        }
        crate::lifecycle::check_transition(obj.lifecycle_state, LifecycleState::Compressed)?;
        let replicas = self.with_inner(|g| g.store.replicas_for(id))?;
        let primary = replicas
            .iter()
            .find(|r| {
                r.valid
                    && r.location.kind != PhysicalKind::Archived
                    && obj
                        .content_hash
                        .map(|hash| r.content_hash == hash)
                        .unwrap_or(true)
            })
            .ok_or_else(|| ReclaimError::NotFound(format!("no live payload for {id}")))?;
        let data = self.read_payload(&primary.location.backend, &primary.location.key)?;
        crate::integrity::verify_sha256(&data, &primary.content_hash)?;
        let codec = ZstdCodec::default();
        let (compressed_bytes, result) = compress_verified_with_bytes(&codec, &data)?;
        if let Some(expected) = obj.content_hash {
            if expected != result.original_hash {
                return Err(ReclaimError::IntegrityFailure(format!(
                    "object content hash {expected} does not match source replica {}",
                    result.original_hash
                )));
            }
        }
        let compressed_hash = ContentHash::of(&compressed_bytes);
        let backend_id = primary.location.backend.clone();
        let _payload_operations = self.begin_payload_operations(vec![
            (backend_id.clone(), primary.content_hash),
            (backend_id.clone(), compressed_hash),
        ])?;

        // Decide the compressed payload's canonical key while the shared
        // payload guard excludes cross-object acquire/release races.
        let existing_new = self.with_inner(|g| g.store.get_dedup(&compressed_hash, &backend_id))?;
        let (new_key, write_payload) = if let Some(entry) = &existing_new {
            if entry.ref_count == 0 {
                return Err(ReclaimError::Dedup(format!(
                    "zero-reference dedup row for {compressed_hash} on {backend_id}"
                )));
            }
            let canonical = self.read_payload(&backend_id, &entry.key)?;
            crate::integrity::verify_sha256(&canonical, &compressed_hash)?;
            (entry.key.clone(), false)
        } else {
            (format!("{}-zstd", primary.location.key), true)
        };
        let owner_node = self.node_for_backend(&backend_id)?;
        if write_payload {
            if let Err(e) = self.write_payload_to(&backend_id, &new_key, &compressed_bytes) {
                return match self.delete_payload(&backend_id, &new_key) {
                    Ok(()) => Err(e),
                    Err(cleanup) => Err(ReclaimError::Internal(format!(
                        "compressed payload write failed ({e}); partial payload cleanup also failed ({cleanup})"
                    ))),
                };
            }
        }
        let old_dedup = if primary.content_hash == ContentHash::empty() {
            None
        } else {
            let entry = self
                .with_inner(|g| g.store.get_dedup(&primary.content_hash, &backend_id))?
                .ok_or_else(|| {
                    ReclaimError::Dedup(format!(
                        "missing source dedup row for {} on {backend_id}",
                        primary.content_hash
                    ))
                })?;
            if entry.key != primary.location.key {
                if write_payload {
                    let _ = self.delete_payload(&backend_id, &new_key);
                }
                return Err(ReclaimError::Dedup(format!(
                    "source dedup key {} disagrees with replica key {}",
                    entry.key, primary.location.key
                )));
            }
            Some(entry)
        };
        let mut updated = obj.clone();
        updated.compressed_size = Some(result.compressed_size);
        updated.lifecycle_state = LifecycleState::Compressed;
        updated.physical_size = result.compressed_size;
        updated.app_metadata.insert(
            COMPRESSION_ORIGINAL_SIZE_KEY.into(),
            serde_json::Value::from(result.original_size),
        );
        let compressed_replica = Replica {
            replica_id: Uuid::new_v4(),
            object_id: obj.id,
            generation: obj.generation,
            location: crate::object::PhysicalLocation {
                backend: backend_id.clone(),
                key: new_key.clone(),
                kind: primary.location.kind,
            },
            size: result.compressed_size,
            content_hash: compressed_hash,
            created_at_ms: self.now_ms(),
            verified_at_ms: Some(self.now_ms()),
            valid: true,
            owner_node,
        };
        let audit = AuditEntry {
            id: 0,
            ts_ms: self.clock.now_ms(),
            actor: actor.into(),
            action: "OBJECT_COMPRESSED".into(),
            object_id: Some(obj.id),
            generation: Some(obj.generation),
            prior_state: Some(obj.lifecycle_state.as_str().into()),
            new_state: Some(updated.lifecycle_state.as_str().into()),
            policy: None,
            attempt_id: None,
            node: None,
            detail: json!({
                "codec": result.codec,
                "original_size": result.original_size,
                "compressed_size": result.compressed_size,
                "compressed_hash": compressed_hash.to_string(),
                "key": new_key,
            }),
        };

        let mut new_ref_recorded = false;
        let mut old_ref_released = false;
        let mut old_replica_deleted = false;
        let mut new_replica_added = false;
        let metadata_result = self.with_inner(|g| {
            if primary.content_hash != compressed_hash {
                if existing_new.is_some() {
                    g.store.dedup_acquire(&compressed_hash, &backend_id)?;
                } else {
                    g.store.insert_dedup(&DedupEntry {
                        content_hash: compressed_hash,
                        backend: backend_id.clone(),
                        key: compressed_replica.location.key.clone(),
                        ref_count: 1,
                        payload_size: result.compressed_size,
                    })?;
                }
                new_ref_recorded = true;
                if old_dedup.is_some() {
                    g.store.dedup_release(&primary.content_hash, &backend_id)?;
                    old_ref_released = true;
                }
            }
            g.store.delete_replica(&primary.replica_id)?;
            old_replica_deleted = true;
            g.store.add_replica(&compressed_replica)?;
            new_replica_added = true;
            g.store.update_object_with_audit(&updated, &audit)
        });
        if let Err(e) = metadata_result {
            let cleanup_result = self.with_inner(|g| {
                if new_replica_added {
                    g.store.delete_replica(&compressed_replica.replica_id)?;
                }
                if old_replica_deleted {
                    g.store.add_replica(primary)?;
                }
                if old_ref_released {
                    let old = old_dedup.as_ref().ok_or_else(|| {
                        ReclaimError::Dedup("missing source dedup rollback snapshot".into())
                    })?;
                    if g.store
                        .get_dedup(&old.content_hash, &old.backend)?
                        .is_some()
                    {
                        g.store.dedup_acquire(&old.content_hash, &old.backend)?;
                    } else {
                        g.store.insert_dedup(old)?;
                    }
                }
                if new_ref_recorded {
                    g.store.dedup_release(&compressed_hash, &backend_id)?;
                }
                Ok(())
            });
            if write_payload {
                let _ = self.delete_payload(&backend_id, &compressed_replica.location.key);
            }
            return match cleanup_result {
                Ok(()) => Err(e),
                Err(cleanup) => Err(ReclaimError::Internal(format!(
                    "compression metadata failed ({e}); rollback also failed ({cleanup})"
                ))),
            };
        }
        if old_dedup.as_ref().is_some_and(|entry| entry.ref_count == 1)
            && primary.content_hash != compressed_hash
        {
            if let Err(e) = self.delete_payload(&backend_id, &primary.location.key) {
                self.with_inner(|g| {
                    g.store.insert_failure(&FailureRecord {
                        id: 0,
                        ts_ms: self.now_ms(),
                        object_id: Some(obj.id),
                        attempt_id: None,
                        kind: "COMPRESSION_SOURCE_CLEANUP_FAILED".into(),
                        message: e.to_string(),
                        recovered: false,
                    })
                })?;
                log::error!(
                    "compression committed for {} but obsolete source cleanup failed: {e}",
                    obj.id
                );
            }
        }
        Ok(result)
    }

    /// Archive the primary payload into the configured archive backend.
    pub fn archive(&self, id: &Uuid, actor: &str) -> Result<ArchiveRecord> {
        let _operation = self.begin_object_operation(id)?;
        let obj = self.with_inner(|g| g.store.require_object(id))?;
        if obj.lifecycle_state == LifecycleState::Archived {
            return self
                .with_inner(|g| g.store.archives_for(id))?
                .into_iter()
                .last()
                .ok_or_else(|| {
                    ReclaimError::IntegrityFailure(format!(
                        "object {id} is ARCHIVED without an archive record"
                    ))
                });
        }
        crate::lifecycle::check_transition(obj.lifecycle_state, LifecycleState::Archived)?;
        let replicas = self.with_inner(|g| g.store.replicas_for(id))?;
        let primary = replicas
            .iter()
            .find(|r| {
                r.valid
                    && r.location.kind != PhysicalKind::Archived
                    && if obj.compressed_size.is_some() {
                        obj.content_hash != Some(r.content_hash)
                    } else {
                        obj.content_hash == Some(r.content_hash)
                    }
            })
            .ok_or_else(|| ReclaimError::NotFound(format!("no live payload for {id}")))?;
        if obj.compressed_size.is_some()
            && obj
                .app_metadata
                .get(COMPRESSION_ORIGINAL_SIZE_KEY)
                .and_then(serde_json::Value::as_u64)
                .is_none()
        {
            return Err(ReclaimError::IntegrityFailure(format!(
                "compressed object {id} has no valid persisted original byte length"
            )));
        }
        let data = self.read_payload(&primary.location.backend, &primary.location.key)?;
        crate::integrity::verify_sha256(&data, &primary.content_hash)?;
        let hash = ContentHash::of(&data);

        let archive = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
            guard.archives.first().cloned().ok_or_else(|| {
                ReclaimError::ArchiveFailure("no archive backend configured".into())
            })?
        };
        let key = format!("{}/{}/gen{}", obj.id, obj.generation, primary.replica_id);
        let size = archive.write(&key, &data, &hash)?;
        let record = ArchiveRecord {
            archive_id: Uuid::new_v4().to_string(),
            object_id: obj.id,
            generation: obj.generation,
            backend: archive.id().to_string(),
            key,
            size,
            content_hash: hash,
            created_at_ms: self.now_ms(),
        };
        if let Err(e) = self.with_inner(|g| g.store.insert_archive(&record)) {
            archive.delete(&record.key).map_err(|cleanup| {
                ReclaimError::Internal(format!(
                    "archive metadata insert failed ({e}); archive payload cleanup also failed ({cleanup})"
                ))
            })?;
            return Err(e);
        }
        let mut archived = obj.clone();
        archived.lifecycle_state = LifecycleState::Archived;
        let metadata_result = self.with_inner(|g| {
            g.store.update_object_with_audit(&archived, &AuditEntry {
                id: 0,
                ts_ms: self.clock.now_ms(),
                actor: actor.into(),
                action: "OBJECT_ARCHIVED".into(),
                object_id: Some(obj.id),
                generation: Some(obj.generation),
                prior_state: Some(obj.lifecycle_state.as_str().into()),
                new_state: Some(archived.lifecycle_state.as_str().into()),
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({"archive_key": record.key, "archive_id": record.archive_id, "size": size}),
            })
        });
        if let Err(e) = metadata_result {
            let row_cleanup = self.with_inner(|g| g.store.delete_archive(&record.archive_id));
            if let Err(cleanup) = row_cleanup {
                return Err(ReclaimError::Internal(format!(
                    "archive object commit failed ({e}); archive-row cleanup also failed ({cleanup})"
                )));
            }
            archive.delete(&record.key).map_err(|cleanup| {
                ReclaimError::Internal(format!(
                    "archive object commit failed ({e}); archive payload cleanup also failed ({cleanup})"
                ))
            })?;
            return Err(e);
        }
        Ok(record)
    }

    /// Restore an archived object back to a hot backend.
    pub fn restore(&self, id: &Uuid, actor: &str) -> Result<ReclaimObject> {
        let _operation = self.begin_object_operation(id)?;
        let obj = self.with_inner(|g| g.store.require_object(id))?;
        if obj.lifecycle_state != LifecycleState::Archived {
            return Err(ReclaimError::InvalidArgument(format!(
                "object {} is not ARCHIVED",
                obj.id
            )));
        }
        let archives = self.with_inner(|g| g.store.archives_for(id))?;
        let record = archives
            .last()
            .ok_or_else(|| ReclaimError::NotFound(format!("no archive record for {id}")))?;
        let archive = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
            guard
                .archives
                .iter()
                .find(|a| a.id() == record.backend)
                .cloned()
                .ok_or_else(|| {
                    ReclaimError::ArchiveFailure(format!(
                        "archive backend {} unavailable",
                        record.backend
                    ))
                })?
        };
        let archived_bytes = archive.read(&record.key)?;
        crate::integrity::verify_sha256(&archived_bytes, &record.content_hash)?;
        let data = if obj.compressed_size.is_some()
            && obj.content_hash.is_some()
            && obj.content_hash != Some(record.content_hash)
        {
            let original_size = obj
                .app_metadata
                .get(COMPRESSION_ORIGINAL_SIZE_KEY)
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    ReclaimError::IntegrityFailure(format!(
                        "compressed object {} has no valid persisted original byte length",
                        obj.id
                    ))
                })?;
            let max_output = usize::try_from(original_size).map_err(|_| {
                ReclaimError::InvalidArgument(
                    "original byte length cannot be represented for decompression".into(),
                )
            })?;
            let restored = ZstdCodec::default().decompress_bounded(&archived_bytes, max_output)?;
            if restored.len() as u64 != original_size {
                return Err(ReclaimError::IntegrityFailure(format!(
                    "restored byte length {} does not match persisted original length {original_size}",
                    restored.len()
                )));
            }
            crate::integrity::verify_sha256(
                &restored,
                &obj.content_hash.ok_or_else(|| {
                    ReclaimError::IntegrityFailure(
                        "compressed object has no original content hash".into(),
                    )
                })?,
            )?;
            restored
        } else {
            if let Some(expected) = obj.content_hash {
                crate::integrity::verify_sha256(&archived_bytes, &expected)?;
            }
            archived_bytes
        };
        let restored_hash = ContentHash::of(&data);

        // Archiving does not evict an existing hot replica. If this object
        // still owns a valid logical copy, restoration is a state transition,
        // not a second ownership/refcount acquisition.
        let existing_hot = self
            .with_inner(|g| g.store.replicas_for(id))?
            .into_iter()
            .find(|replica| {
                replica.valid
                    && replica.location.kind != PhysicalKind::Archived
                    && replica.content_hash == restored_hash
            });
        if let Some(existing) = existing_hot {
            let existing_bytes =
                self.read_payload(&existing.location.backend, &existing.location.key)?;
            crate::integrity::verify_sha256(&existing_bytes, &restored_hash)?;
            let mut restored = obj.clone();
            restored.lifecycle_state = LifecycleState::Warm;
            restored.physical_size = existing.size;
            restored.compressed_size = None;
            restored.app_metadata.remove(COMPRESSION_ORIGINAL_SIZE_KEY);
            restored.replication_count = u32::try_from(
                self.with_inner(|g| g.store.valid_replica_count(id))?,
            )
            .map_err(|_| ReclaimError::InvalidArgument("replica count exceeds u32".into()))?;
            self.with_inner(|g| {
                g.store.update_object(&restored)?;
                g.store.append_audit(&AuditEntry {
                    id: 0,
                    ts_ms: self.clock.now_ms(),
                    actor: actor.into(),
                    action: "OBJECT_RESTORED".into(),
                    object_id: Some(obj.id),
                    generation: Some(obj.generation),
                    prior_state: Some(LifecycleState::Archived.as_str().into()),
                    new_state: Some(LifecycleState::Warm.as_str().into()),
                    policy: None,
                    attempt_id: None,
                    node: None,
                    detail: json!({
                        "backend": existing.location.backend,
                        "key": existing.location.key,
                        "reused_existing": true,
                    }),
                })?;
                Ok(())
            })?;
            return Ok(restored);
        }

        let hot_backend = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
            let mut candidates = Vec::new();
            for id in guard.backends.ids()? {
                let priority = match guard.backends.get(&id)?.kind() {
                    "memory" => 0,
                    "file" => 1,
                    _ => continue,
                };
                candidates.push((priority, id));
            }
            candidates.sort();
            candidates
                .into_iter()
                .map(|(_, id)| id)
                .next()
                .ok_or_else(|| ReclaimError::Backend("no hot backend configured".into()))?
        };
        let _payload_operation =
            self.begin_payload_operations(vec![(hot_backend.clone(), restored_hash)])?;
        let existing = self.with_inner(|g| g.store.get_dedup(&restored_hash, &hot_backend))?;
        let (hot_key, wrote_payload) = if let Some(entry) = &existing {
            if entry.ref_count == 0 {
                return Err(ReclaimError::Dedup(format!(
                    "zero-reference dedup row for {} on {hot_backend}",
                    restored_hash
                )));
            }
            let canonical = self.read_payload(&hot_backend, &entry.key)?;
            crate::integrity::verify_sha256(&canonical, &restored_hash)?;
            (entry.key.clone(), false)
        } else {
            let key = format!("{}-restored", record.key.replace(['/', '\\'], "-"));
            self.write_payload_to(&hot_backend, &key, &data)?;
            (key, true)
        };
        let replica = Replica {
            replica_id: Uuid::new_v4(),
            object_id: obj.id,
            generation: obj.generation,
            location: crate::object::PhysicalLocation {
                backend: hot_backend.clone(),
                key: hot_key.clone(),
                kind: PhysicalKind::Hot,
            },
            size: data.len() as u64,
            content_hash: restored_hash,
            created_at_ms: self.now_ms(),
            verified_at_ms: Some(self.now_ms()),
            valid: true,
            owner_node: self.node_for_backend(&hot_backend)?,
        };
        let mut restored = obj.clone();
        restored.lifecycle_state = LifecycleState::Warm;
        restored.physical_size = data.len() as u64;
        restored.compressed_size = None;
        restored.app_metadata.remove(COMPRESSION_ORIGINAL_SIZE_KEY);
        let mut dedup_recorded = false;
        let mut replica_recorded = false;
        let mut object_updated = false;
        let metadata_result = self.with_inner(|g| {
            if existing.is_some() {
                g.store.dedup_acquire(&restored_hash, &hot_backend)?;
            } else {
                g.store.insert_dedup(&DedupEntry {
                    content_hash: restored_hash,
                    backend: hot_backend.clone(),
                    key: replica.location.key.clone(),
                    ref_count: 1,
                    payload_size: replica.size,
                })?;
            }
            dedup_recorded = true;
            g.store.add_replica(&replica)?;
            replica_recorded = true;
            restored.replication_count = u32::try_from(g.store.valid_replica_count(id)?)
                .map_err(|_| ReclaimError::InvalidArgument("replica count exceeds u32".into()))?;
            g.store.update_object(&restored)?;
            object_updated = true;
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: self.clock.now_ms(),
                actor: actor.into(),
                action: "OBJECT_RESTORED".into(),
                object_id: Some(obj.id),
                generation: Some(obj.generation),
                prior_state: Some(LifecycleState::Archived.as_str().into()),
                new_state: Some(LifecycleState::Warm.as_str().into()),
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({"backend": replica.location.backend, "key": replica.location.key}),
            })?;
            Ok(())
        });
        if let Err(e) = metadata_result {
            let delete_payload = self.with_inner(|g| {
                if object_updated {
                    g.store.update_object(&obj)?;
                }
                if replica_recorded {
                    g.store.delete_replica(&replica.replica_id)?;
                }
                if dedup_recorded {
                    g.store.dedup_release(&restored_hash, &hot_backend)
                } else {
                    Ok(wrote_payload)
                }
            })?;
            if delete_payload {
                self.delete_payload(&hot_backend, &hot_key)
                    .map_err(|cleanup| {
                        ReclaimError::Internal(format!(
                        "restore metadata failed ({e}); payload cleanup also failed ({cleanup})"
                    ))
                    })?;
            }
            return Err(e);
        }
        Ok(restored)
    }

    /// Verify integrity of every replica of an object.
    pub fn verify(&self, id: &Uuid, actor: &str) -> Result<serde_json::Value> {
        let _operation = self.begin_object_operation(id)?;
        let obj = self.with_inner(|g| g.store.require_object(id))?;
        let replicas = self.with_inner(|g| g.store.replicas_for(id))?;
        let _payload_operations = self.begin_payload_operations(
            replicas
                .iter()
                .map(|replica| (replica.location.backend.clone(), replica.content_hash))
                .collect(),
        )?;
        let mut results = Vec::new();
        for replica in &replicas {
            let verified = match self.read_payload(&replica.location.backend, &replica.location.key)
            {
                Ok(data) => crate::integrity::verify_sha256(&data, &replica.content_hash),
                Err(e) => Err(e),
            };
            match verified {
                Ok(()) => {
                    results.push(json!({"replica": replica.replica_id.to_string(), "ok": true}));
                    let mut r = replica.clone();
                    r.verified_at_ms = Some(self.now_ms());
                    r.valid = true;
                    self.with_inner(|g| {
                        g.store.update_replica(&r)?;
                        Ok(())
                    })?;
                }
                Err(e) => {
                    results.push(
                        json!({"replica": replica.replica_id.to_string(), "ok": false, "error": e.to_string()}),
                    );
                    self.with_inner(|g| {
                        // A deduplicated physical key is shared truth. If it
                        // is corrupt, every metadata owner of that exact
                        // backend/key/hash must be invalidated together.
                        for mut affected in g.store.all_replicas()?.into_iter().filter(|r| {
                            r.valid
                                && r.location.backend == replica.location.backend
                                && r.location.key == replica.location.key
                                && r.content_hash == replica.content_hash
                        }) {
                            affected.valid = false;
                            g.store.update_replica(&affected)?;
                        }
                        g.store.insert_failure(&FailureRecord {
                            id: 0,
                            ts_ms: self.clock.now_ms(),
                            object_id: Some(obj.id),
                            attempt_id: None,
                            kind: "INTEGRITY_FAILURE".into(),
                            message: e.to_string(),
                            recovered: false,
                        })?;
                        Ok(())
                    })?;
                }
            }
        }
        self.with_inner(|g| {
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: self.clock.now_ms(),
                actor: actor.into(),
                action: "OBJECT_VERIFIED".into(),
                object_id: Some(obj.id),
                generation: Some(obj.generation),
                prior_state: None,
                new_state: None,
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({"results": results}),
            })?;
            Ok(())
        })?;
        Ok(json!({"object_id": obj.id.to_string(), "results": results}))
    }

    // ------------------------------------------------------------------
    // Pressure
    // ------------------------------------------------------------------

    pub fn pressure(&self) -> Result<PressureMetrics> {
        self.with_inner(|g| g.pressure.try_aggregate())
    }

    pub fn pressure_level(&self) -> Result<PressureLevel> {
        self.with_inner(|g| g.pressure.try_level())
    }

    /// Set pressure on a synthetic provider (demo/test path).
    pub fn set_pressure(&self, provider_id: &str, level: PressureLevel) -> Result<()> {
        self.with_inner(|g| {
            let provider = g.pressure.find_synthetic(provider_id).ok_or_else(|| {
                ReclaimError::NotFound(format!("pressure provider {provider_id}"))
            })?;
            provider.set_level(level)?;
            Ok(())
        })
    }

    pub fn register_synthetic_pressure(&self, id: &str) -> Result<()> {
        self.with_inner(|g| {
            g.pressure.register_synthetic(id);
            Ok(())
        })
    }

    // ------------------------------------------------------------------
    // Nodes
    // ------------------------------------------------------------------

    pub fn node_register(&self, req: &NodeRegisterRequest) -> Result<NodeRegisterReply> {
        if req.name.trim().is_empty()
            || req.name.trim() != req.name
            || req.process_id.trim().is_empty()
            || req.process_id.trim() != req.process_id
        {
            return Err(ReclaimError::InvalidArgument(
                "node name and process id must be non-empty and have no surrounding whitespace"
                    .into(),
            ));
        }
        let namespace = format!("{}/", req.name);
        let mut backend_ids = HashSet::new();
        for backend in &req.backends {
            if !backend.id.starts_with(&namespace) || backend.id.len() == namespace.len() {
                return Err(ReclaimError::InvalidArgument(format!(
                    "node backend {} must be namespaced under {namespace}",
                    backend.id
                )));
            }
            if backend.kind.trim().is_empty() {
                return Err(ReclaimError::InvalidArgument(format!(
                    "node backend {} has an empty kind",
                    backend.id
                )));
            }
            if !backend_ids.insert(backend.id.as_str()) {
                return Err(ReclaimError::InvalidArgument(format!(
                    "node registration contains duplicate backend {}",
                    backend.id
                )));
            }
        }
        let now = self.now_ms();
        self.with_inner(|g| {
            let node_id = format!("{}@{}@{}", req.name, req.process_id, req.boot_id);
            if req.addr.trim().is_empty() || req.addr.trim() != req.addr {
                return Err(ReclaimError::InvalidArgument(
                    "node must provide its numeric listener address without surrounding whitespace"
                        .into(),
                ));
            }
            let addr = req.addr.clone();
            let socket_addr: std::net::SocketAddr = addr.parse().map_err(|e| {
                ReclaimError::InvalidArgument(format!("invalid node listen address {addr}: {e}"))
            })?;
            if socket_addr.port() == 0 {
                return Err(ReclaimError::InvalidArgument(
                    "node listen address must have a non-zero port".into(),
                ));
            }

            // Retire expired registrations before checking the unique logical
            // node name. This permits a crashed node to restart after the
            // configured lease while preventing two live registrations from
            // racing on the same name-derived backend namespace.
            let timeout = g.config.node_heartbeat_timeout_ms;
            let stale: Vec<String> = g
                .nodes
                .iter()
                .filter(|(_, node)| now.saturating_sub(node.last_seen_ms) > timeout)
                .map(|(id, _)| id.clone())
                .collect();
            for stale_id in stale {
                g.nodes.remove(&stale_id);
                g.pressure.remove_node(&stale_id);
                g.store.append_audit(&AuditEntry {
                    id: 0,
                    ts_ms: now,
                    actor: "coordinator".into(),
                    action: "NODE_RETIRED".into(),
                    object_id: None,
                    generation: None,
                    prior_state: None,
                    new_state: None,
                    policy: None,
                    attempt_id: None,
                    node: Some(stale_id),
                    detail: json!({"reason": "heartbeat timeout"}),
                })?;
            }
            if let Some(existing) = g
                .nodes
                .values()
                .find(|node| node.name == req.name && node.node_id != node_id)
            {
                return Err(ReclaimError::ReservationConflict(format!(
                    "node name {} is already held by live registration {}",
                    req.name, existing.node_id
                )));
            }
            if let Some(existing) = g.nodes.get(&node_id) {
                let mut info = existing.clone();
                info.last_seen_ms = now;
                info.addr = addr.clone();
                info.backends = req.backends.clone();
                g.nodes.insert(node_id.clone(), info);
                return Ok(NodeRegisterReply {
                    node_id,
                    coordinator_epoch: g.epoch,
                    heartbeat_interval_ms: 5_000,
                    heartbeat_timeout_ms: g.config.node_heartbeat_timeout_ms as u64,
                });
            }
            g.nodes.insert(
                node_id.clone(),
                NodeInfo {
                    node_id: node_id.clone(),
                    name: req.name.clone(),
                    process_id: req.process_id.clone(),
                    boot_id: req.boot_id,
                    addr: addr.clone(),
                    backends: req.backends.clone(),
                    last_seen_ms: now,
                },
            );
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: now,
                actor: "node".into(),
                action: "NODE_REGISTERED".into(),
                object_id: None,
                generation: None,
                prior_state: None,
                new_state: None,
                policy: None,
                attempt_id: None,
                node: Some(node_id.clone()),
                detail: json!({
                    "name": req.name,
                    "addr": addr,
                    "backends": req.backends.iter().map(|b| b.id.clone()).collect::<Vec<_>>(),
                }),
            })?;
            Ok(NodeRegisterReply {
                node_id,
                coordinator_epoch: g.epoch,
                heartbeat_interval_ms: 5_000,
                heartbeat_timeout_ms: g.config.node_heartbeat_timeout_ms as u64,
            })
        })
    }

    pub fn node_heartbeat(&self, node_id: &str) -> Result<u64> {
        let now = self.now_ms();
        self.with_inner(|g| {
            let info = g
                .nodes
                .get_mut(node_id)
                .ok_or_else(|| ReclaimError::NotFound(format!("node {node_id}")))?;
            info.last_seen_ms = info.last_seen_ms.max(now);
            Ok(g.epoch)
        })
    }

    pub fn node_report_pressure(&self, node_id: &str, metrics: PressureMetrics) -> Result<()> {
        metrics.validate()?;
        let now = self.now_ms();
        self.with_inner(|g| {
            g.nodes
                .get(node_id)
                .ok_or_else(|| ReclaimError::NotFound(format!("node {node_id}")))?;
            g.pressure.report_node(node_id, metrics)
        })?;
        log::debug!("pressure from {node_id} at {now}: {:?}", metrics);
        Ok(())
    }

    pub fn nodes(&self) -> Result<Vec<NodeInfo>> {
        self.with_inner(|g| {
            let mut v: Vec<NodeInfo> = g.nodes.values().cloned().collect();
            v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
            Ok(v)
        })
    }

    /// Retire nodes whose heartbeat expired ("connection retirement").
    pub fn retire_stale_nodes(&self) -> Result<Vec<String>> {
        let now = self.now_ms();
        self.with_inner(|g| {
            let timeout = g.config.node_heartbeat_timeout_ms;
            let stale: Vec<String> = g
                .nodes
                .iter()
                .filter(|(_, n)| now.saturating_sub(n.last_seen_ms) > timeout)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                g.nodes.remove(id);
                g.pressure.remove_node(id);
                g.store.append_audit(&AuditEntry {
                    id: 0,
                    ts_ms: now,
                    actor: "coordinator".into(),
                    action: "NODE_RETIRED".into(),
                    object_id: None,
                    generation: None,
                    prior_state: None,
                    new_state: None,
                    policy: None,
                    attempt_id: None,
                    node: Some(id.clone()),
                    detail: json!({"reason": "heartbeat timeout"}),
                })?;
            }
            Ok(stale)
        })
    }

    // ------------------------------------------------------------------
    // Recovery / misc
    // ------------------------------------------------------------------

    /// Reconcile the store against physical truth.
    pub fn recover(&self) -> Result<crate::recovery::RecoveryReport> {
        let _recovery = self.begin_recovery()?;
        let report = reconcile_store(&self.store()?, self.now_ms(), &|payload| {
            // Determine whether scheduled deletions are physically gone.
            let (_replica_deletions, deletions, archive_deletions) =
                crate::recovery::parse_journal_deletions(payload)?;
            if deletions.is_empty() && archive_deletions.is_empty() {
                return Ok(true);
            }
            let mut any_exists = false;
            let mut any_missing = false;
            for r in &deletions {
                if self.payload_exists(&r.location.backend, &r.location.key)? {
                    any_exists = true;
                } else {
                    any_missing = true;
                }
                if any_exists && any_missing {
                    return Err(ReclaimError::Recovery(
                        "journal deletions have mixed physical state; refusing to restore missing data or commit present data".into(),
                    ));
                }
            }
            for a in &archive_deletions {
                let backend = {
                    let guard = self
                        .inner
                        .lock()
                        .map_err(|_| ReclaimError::Internal("coordinator poisoned".into()))?;
                    guard
                        .archives
                        .iter()
                        .find(|b| b.id() == a.backend)
                        .cloned()
                        .ok_or_else(|| {
                            ReclaimError::Recovery(format!(
                                "archive backend {} is unavailable during recovery",
                                a.backend
                            ))
                        })?
                };
                if backend.exists(&a.key)? {
                    any_exists = true;
                } else {
                    any_missing = true;
                }
                if any_exists && any_missing {
                    return Err(ReclaimError::Recovery(
                        "journal deletions have mixed physical state; refusing to restore missing data or commit present data".into(),
                    ));
                }
            }
            Ok(any_exists)
        })?;
        let recovery_detail = serde_json::to_value(&report).map_err(ReclaimError::from)?;
        self.with_inner(|g| {
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: self.now_ms(),
                actor: "coordinator".into(),
                action: "RECOVERY_RAN".into(),
                object_id: None,
                generation: None,
                prior_state: None,
                new_state: None,
                policy: None,
                attempt_id: None,
                node: None,
                detail: recovery_detail,
            })?;
            Ok(())
        })?;
        Ok(report)
    }

    pub fn stats(&self) -> Result<serde_json::Value> {
        let mut stats = self.with_inner(|g| g.store.stats())?;
        stats["epoch"] = serde_json::json!(self.epoch());
        stats["nodes"] = serde_json::json!(self.nodes()?.len());
        stats["pressure"] = serde_json::json!(self.pressure()?);
        stats["pressure_level"] = serde_json::json!(self.pressure_level()?.as_str());
        Ok(stats)
    }

    pub fn policy_registry(&self) -> Result<PolicyRegistry> {
        self.with_inner(|g| Ok(g.policies.clone()))
    }

    pub fn add_policy(&self, policy: Policy) -> Result<()> {
        crate::policy::validate_policy(&policy)?;
        self.with_inner(|g| {
            let mut prospective = g.policies.clone();
            prospective.add(policy.clone())?;
            prospective.validate_complete()?;
            // Persist first: if SQLite rejects the write, the live registry is
            // unchanged. Replacing the in-memory registry is then infallible.
            g.store.upsert_policy(&policy)?;
            g.policies = prospective;
            g.store.append_audit(&AuditEntry {
                id: 0,
                ts_ms: self.now_ms(),
                actor: "cli".into(),
                action: "POLICY_ADDED".into(),
                object_id: None,
                generation: None,
                prior_state: None,
                new_state: None,
                policy: Some(policy.full_id()),
                attempt_id: None,
                node: None,
                detail: json!({"kind": format!("{:?}", policy.kind)}),
            })?;
            Ok(())
        })
    }

    pub fn failures(&self, limit: u64) -> Result<Vec<FailureRecord>> {
        self.with_inner(|g| g.store.list_failures(limit))
    }

    pub fn audit(
        &self,
        object_id: Option<&Uuid>,
        action: Option<&str>,
        limit: u64,
    ) -> Result<Vec<crate::persistence::AuditEntry>> {
        self.with_inner(|g| g.store.replay_audit(object_id, action, limit))
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        let _ = self.stop_authority_heartbeat();
        if self
            .authority_lost
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        if let Ok(inner) = self.inner.lock() {
            let _ = inner.store.release_coordinator(
                &inner.config.process_id,
                &inner.boot_id,
                inner.epoch,
            );
        }
    }
}

fn decode_base64(s: &str) -> Result<Vec<u8>> {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    STANDARD
        .decode(s)
        .map_err(|e| ReclaimError::Protocol(format!("bad base64: {e}")))
}

fn encode_base64(data: &[u8]) -> String {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    STANDARD.encode(data)
}
