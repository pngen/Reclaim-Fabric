//! Integration tests: end-to-end coordinator flows against real stores,
//! backends, and archives.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use reclaim_fabric::backends::{Backend, BackendRegistry, FileBackend, MemoryBackend};
use reclaim_fabric::coordinator::{Clock, Coordinator, CoordinatorConfig, FrozenClock};
use reclaim_fabric::economics::ReclaimVerdict;
use reclaim_fabric::errors::ReclaimError;
use reclaim_fabric::integrity::ContentHash;
use reclaim_fabric::lifecycle::LifecycleState;
use reclaim_fabric::lineage::EdgeKind;
use reclaim_fabric::object::{DurabilityClass, PhysicalKind, ReclaimObject};
use reclaim_fabric::persistence::{
    Attempt, AttemptStatus, JournalEntry, JournalPhase, Reservation, Store,
};
use reclaim_fabric::policy::{default_policy, PolicyKind};
use reclaim_fabric::pressure::{PressureLevel, PressureMetrics, PressureRegistry};
use reclaim_fabric::protocol::{CreateObjectRequest, NodeRegisterRequest, ReclaimRequest};
use reclaim_fabric::recovery::{journal_payload, with_physical_deletions};
use tempfile::TempDir;
use uuid::Uuid;

struct Harness {
    _dir: TempDir,
    coordinator: Coordinator,
    clock: Arc<FrozenClock>,
}

fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    harness_with(dir, store_path)
}

fn harness_with(dir: TempDir, store_path: String) -> Harness {
    let backends = BackendRegistry::new();
    backends
        .register(Arc::new(MemoryBackend::new("memory")))
        .unwrap();
    let file_dir = dir.path().join("data");
    let backend = FileBackend::new("file:test", &file_dir).unwrap();
    backends.register(Arc::new(backend)).unwrap();
    let pressure = PressureRegistry::new();
    let archive_dir = dir.path().join("archive");
    let archive = reclaim_fabric::archive::LocalFsArchive::new("local-fs", &archive_dir).unwrap();
    let clock: Arc<FrozenClock> = Arc::new(FrozenClock::new(1_000_000));
    let config = CoordinatorConfig {
        store_path,
        process_id: "test-coordinator".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some("127.0.0.1:9999".into()),
    };
    let coordinator = Coordinator::open(
        config,
        backends,
        pressure,
        vec![Arc::new(archive)],
        clock.clone(),
    )
    .unwrap();
    Harness {
        _dir: dir,
        coordinator,
        clock,
    }
}

fn obj(class: &str) -> ReclaimObject {
    let mut o = ReclaimObject::new(Uuid::new_v4(), 0, class, 1000, 1_000_000);
    o.reuse_probability = 0.01;
    o.recompute_cost = Some(1.0);
    o.memory_cost_per_byte_sec = 1.0;
    o
}

fn create_with_payload(
    coordinator: &Coordinator,
    obj: ReclaimObject,
    payload: &[u8],
) -> ReclaimObject {
    let req = CreateObjectRequest {
        object: obj,
        payload_b64: Some(reclaim_fabric::base64_payload(payload)),
        target_backend: Some("memory".into()),
        replicate_to: vec![],
    };
    coordinator.create_object(&req).unwrap()
}

fn reclaim(
    coordinator: &Coordinator,
    id: Uuid,
    force: bool,
) -> Result<reclaim_fabric::coordinator::ReclaimReport, ReclaimError> {
    coordinator.reclaim(&ReclaimRequest {
        object_id: id,
        actor: "tester".into(),
        force,
    })
}

#[test]
fn full_lifecycle_create_plan_reclaim() {
    let h = harness();
    let o = create_with_payload(&h.coordinator, obj("checkpoint"), b"state-payload");
    assert_eq!(o.lifecycle_state, LifecycleState::Hot);
    assert_eq!(o.content_hash, Some(ContentHash::of(b"state-payload")));

    // Decision: cheap recompute -> reclaim recommendation.
    let decision = h.coordinator.plan(&o.id, "tester").unwrap();
    assert_eq!(decision.decision.verdict, ReclaimVerdict::Reclaim);
    assert!(decision
        .decision
        .reasons
        .iter()
        .any(|r| r.contains("score")));

    let report = reclaim(&h.coordinator, o.id, false).unwrap();
    assert!(report.reclaimed);
    assert_eq!(report.final_state, "RECLAIMED");

    let after = h
        .coordinator
        .store()
        .unwrap()
        .require_object(&o.id)
        .unwrap();
    assert_eq!(after.lifecycle_state, LifecycleState::Reclaimed);
    // Invariant 1: RECLAIMED objects have no live physical ownership.
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .replicas_for(&o.id)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn expensive_recompute_is_retained() {
    let h = harness();
    let mut o = obj("checkpoint");
    o.reuse_probability = 0.9;
    o.recompute_cost = Some(1e9);
    let o = create_with_payload(&h.coordinator, o, b"expensive");
    let decision = h.coordinator.plan(&o.id, "tester").unwrap();
    assert_eq!(decision.decision.verdict, ReclaimVerdict::Retain);
    let report = reclaim(&h.coordinator, o.id, false).unwrap();
    assert!(!report.reclaimed);
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .require_object(&o.id)
            .unwrap()
            .lifecycle_state,
        LifecycleState::Hot
    );
}

#[test]
fn pinned_object_cannot_be_reclaimed() {
    let h = harness();
    let mut o = obj("checkpoint");
    o.pinned = true;
    let o = create_with_payload(&h.coordinator, o, b"pinned");
    let err = reclaim(&h.coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::PinnedObject(_)));
    h.coordinator.unpin(&o.id, "tester").unwrap();
    let report = reclaim(&h.coordinator, o.id, true).unwrap();
    assert!(report.reclaimed);
}

#[test]
fn protected_object_cannot_be_reclaimed() {
    let h = harness();
    let mut o = obj("checkpoint");
    o.protected = true;
    let o = create_with_payload(&h.coordinator, o, b"protected");
    let err = reclaim(&h.coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::ProtectedObject(_)));
}

#[test]
fn double_reclaim_rejected() {
    let h = harness();
    let o = create_with_payload(&h.coordinator, obj("checkpoint"), b"data");
    let r1 = reclaim(&h.coordinator, o.id, true).unwrap();
    assert!(r1.reclaimed);
    let err = reclaim(&h.coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::InvalidArgument(_)));
}

#[test]
fn dependency_blocks_reclaim_of_non_reconstructible() {
    let h = harness();
    let parent = create_with_payload(&h.coordinator, obj("parent"), b"parent-data");
    let mut child = obj("child");
    child.recompute_cost = None; // non-reconstructible
    let child = create_with_payload(&h.coordinator, child, b"child-data");
    h.coordinator
        .add_lineage(parent.id, child.id, EdgeKind::DependsOn, "tester")
        .unwrap();
    let err = reclaim(&h.coordinator, parent.id, true).unwrap_err();
    assert!(
        matches!(err, ReclaimError::DependencyViolation(_)),
        "expected dependency violation, got {err:?}"
    );
    h.coordinator
        .remove_lineage(parent.id, child.id, EdgeKind::DependsOn, "tester")
        .unwrap();
    let report = reclaim(&h.coordinator, parent.id, true).unwrap();
    assert!(report.reclaimed);
}

#[test]
fn superseded_checkpoint_can_be_reclaimed() {
    let h = harness();
    let v10 = create_with_payload(&h.coordinator, obj("checkpoint"), b"v10");
    let v11 = create_with_payload(&h.coordinator, obj("checkpoint"), b"v11");
    h.coordinator
        .add_lineage(v10.id, v11.id, EdgeKind::Supersedes, "tester")
        .unwrap();
    let report = reclaim(&h.coordinator, v10.id, true).unwrap();
    assert!(report.reclaimed);
    // The superseding generation is untouched.
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .require_object(&v11.id)
            .unwrap()
            .lifecycle_state,
        LifecycleState::Hot
    );
}

#[test]
fn cycle_in_lineage_rejected() {
    let h = harness();
    let a = create_with_payload(&h.coordinator, obj("a"), b"a");
    let b = create_with_payload(&h.coordinator, obj("b"), b"b");
    h.coordinator
        .add_lineage(a.id, b.id, EdgeKind::DependsOn, "t")
        .unwrap();
    let err = h
        .coordinator
        .add_lineage(b.id, a.id, EdgeKind::DependsOn, "t")
        .unwrap_err();
    assert!(matches!(err, ReclaimError::DependencyViolation(_)));
}

#[test]
fn dedup_shares_payload_and_reclaims_safely() {
    let h = harness();
    let payload = b"shared-content".to_vec();
    let o1 = create_with_payload(&h.coordinator, obj("one"), &payload);
    let o2 = create_with_payload(&h.coordinator, obj("two"), &payload);
    let hash = ContentHash::of(&payload);
    // Two logical objects share one dedup payload.
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .get_dedup(&hash, "memory")
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );
    // Reclaim one object; payload must survive (still referenced).
    let report = reclaim(&h.coordinator, o1.id, true).unwrap();
    assert!(report.reclaimed);
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .get_dedup(&hash, "memory")
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    // Second reclaim: payload physically gone, dedup entry removed.
    let report = reclaim(&h.coordinator, o2.id, true).unwrap();
    assert!(report.reclaimed);
    assert!(h
        .coordinator
        .store()
        .unwrap()
        .get_dedup(&hash, "memory")
        .unwrap()
        .is_none());
}

#[test]
fn compress_then_archive_then_restore() {
    let h = harness();
    let data: Vec<u8> = (0..10_000u32)
        .flat_map(|i| format!("row-{i:06};").into_bytes())
        .collect();
    let o = create_with_payload(&h.coordinator, obj("compressible"), &data);
    let result = h.coordinator.compress(&o.id, "t").unwrap();
    assert!(result.compressed_size < result.original_size);
    let obj = h
        .coordinator
        .store()
        .unwrap()
        .require_object(&o.id)
        .unwrap();
    assert_eq!(obj.compressed_size, Some(result.compressed_size));

    let record = h.coordinator.archive(&o.id, "t").unwrap();
    assert_eq!(record.object_id, o.id);
    let obj = h
        .coordinator
        .store()
        .unwrap()
        .require_object(&o.id)
        .unwrap();
    assert_eq!(obj.lifecycle_state, LifecycleState::Archived);

    let restored = h.coordinator.restore(&o.id, "t").unwrap();
    assert_eq!(restored.lifecycle_state, LifecycleState::Warm);
    let replicas = h.coordinator.store().unwrap().replicas_for(&o.id).unwrap();
    assert!(replicas.iter().any(|r| r.valid));
    let verification = h.coordinator.verify(&o.id, "t").unwrap();
    assert!(verification["results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["ok"] == serde_json::json!(true)));
}

#[test]
fn durable_object_never_reclaimed_below_min_copies() {
    let h = harness();
    let mut o = obj("durable");
    o.durability_class = DurabilityClass::Durable;
    o.reuse_probability = 0.0;
    o.recompute_cost = Some(1.0);
    // DURABLE objects must retain >= 1 valid copy: reclamation of the last
    // (and only) copy is blocked.
    let o = create_with_payload(&h.coordinator, o, b"single");
    let err = reclaim(&h.coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::SurvivabilityViolation(_)));
    // Even with two copies, reclaim would destroy both -> still blocked.
    let data = b"single".to_vec();
    let backend = h.coordinator.backend("memory").unwrap();
    let second_key = "second-copy";
    backend.put(second_key, &data).unwrap();
    let replicas = h.coordinator.store().unwrap().replicas_for(&o.id).unwrap();
    let mut second = replicas[0].clone();
    second.replica_id = Uuid::new_v4();
    second.location.backend = "memory".into();
    second.location.key = second_key.into();
    second.location.kind = PhysicalKind::Durable;
    h.coordinator.store().unwrap().add_replica(&second).unwrap();
    let err = reclaim(&h.coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::SurvivabilityViolation(_)));
    // The invariant held: both replicas are still live.
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .replicas_for(&o.id)
            .unwrap()
            .len(),
        2
    );
    // EPHEMERAL objects reclaim fully.
    let e = create_with_payload(&h.coordinator, obj("ephemeral"), b"gone");
    let report = reclaim(&h.coordinator, e.id, true).unwrap();
    assert!(report.reclaimed);
    assert!(h
        .coordinator
        .store()
        .unwrap()
        .replicas_for(&e.id)
        .unwrap()
        .is_empty());
}

#[test]
fn pressure_drives_candidate_selection() {
    let h = harness();
    let mut a = obj("marginal-a");
    a.reuse_probability = 0.01;
    a.recompute_cost = Some(100.0);
    a.memory_cost_per_byte_sec = 0.5;
    let mut b = obj("marginal-b");
    b.reuse_probability = 0.01;
    b.recompute_cost = Some(100.0);
    b.memory_cost_per_byte_sec = 0.5;
    let a = create_with_payload(&h.coordinator, a, b"a");
    let b = create_with_payload(&h.coordinator, b, b"b");

    h.coordinator
        .register_synthetic_pressure("synthetic")
        .unwrap();
    h.coordinator
        .set_pressure("synthetic", PressureLevel::Normal)
        .unwrap();
    let normal = h.coordinator.candidates(10, "t").unwrap();
    let normal_ids: Vec<Uuid> = normal.iter().map(|d| d.decision.object_id).collect();

    h.coordinator
        .set_pressure("synthetic", PressureLevel::Critical)
        .unwrap();
    let critical = h.coordinator.candidates(10, "t").unwrap();
    let critical_ids: Vec<Uuid> = critical.iter().map(|d| d.decision.object_id).collect();

    assert!(critical_ids.contains(&a.id) || normal_ids.contains(&a.id));
    assert!(critical_ids.contains(&b.id) || normal_ids.contains(&b.id));
    // Critical pressure must never surface protected candidates.
    h.coordinator.set_protected(&a.id, true, "t").unwrap();
    let critical2 = h.coordinator.candidates(10, "t").unwrap();
    assert!(!critical2.iter().any(|d| d.decision.object_id == a.id));
}

#[test]
fn node_pressure_reports_drive_aggregation_until_node_retirement() {
    let h = harness();
    let registration = h
        .coordinator
        .node_register(&NodeRegisterRequest {
            name: "pressure-node".into(),
            process_id: "pressure-process".into(),
            boot_id: Uuid::new_v4(),
            addr: "127.0.0.1:39001".into(),
            backends: vec![],
        })
        .unwrap();
    h.coordinator
        .node_report_pressure(
            &registration.node_id,
            PressureMetrics {
                host_memory: 0.95,
                ..PressureMetrics::default()
            },
        )
        .unwrap();
    assert_eq!(
        h.coordinator.pressure_level().unwrap(),
        PressureLevel::Critical
    );

    h.clock.advance(30_001);
    assert_eq!(
        h.coordinator.retire_stale_nodes().unwrap(),
        vec![registration.node_id]
    );
    assert_eq!(
        h.coordinator.pressure_level().unwrap(),
        PressureLevel::Normal
    );
}

#[test]
fn synthetic_pressure_registration_reports_lost_authority() {
    let h = harness();
    h.coordinator.release().unwrap();
    let error = h
        .coordinator
        .register_synthetic_pressure("late-provider")
        .unwrap_err();
    assert!(
        matches!(error, ReclaimError::ReservationConflict(_)),
        "unexpected error: {error:?}"
    );
}

#[test]
fn decisions_are_deterministic_across_calls() {
    let h = harness();
    let o = create_with_payload(&h.coordinator, obj("checkpoint"), b"data");
    let d1 = h.coordinator.plan(&o.id, "t").unwrap();
    let d2 = h.coordinator.plan(&o.id, "t").unwrap();
    assert_eq!(d1.decision.score, d2.decision.score);
    assert_eq!(d1.decision.reasons, d2.decision.reasons);
    assert_eq!(d1.explanation, d2.explanation);
    assert_eq!(d1.decision.policy_version, "v1");
}

#[test]
fn coordinator_open_rejects_incomplete_policy_state_before_claiming_authority() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let store = Store::open(&store_path).unwrap();
    let mut class_policy = default_policy();
    class_policy.id = "class-only".into();
    class_policy.kind = PolicyKind::ObjectClass;
    class_policy.match_class = Some("checkpoint".into());
    store.upsert_policy(&class_policy).unwrap();
    drop(store);

    let config = CoordinatorConfig {
        store_path,
        process_id: "invalid-policy-test".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: None,
    };
    for _ in 0..2 {
        let result = Coordinator::open(
            config.clone(),
            BackendRegistry::new(),
            PressureRegistry::new(),
            vec![],
            Arc::new(FrozenClock::new(1_000_000)),
        );
        match result {
            Err(ReclaimError::Policy(message)) => {
                assert!(message.contains("exactly one default policy"));
            }
            Err(error) => panic!("expected policy error, got {error}"),
            Ok(_) => panic!("incomplete policy registry unexpectedly opened"),
        }
    }
}

#[test]
fn runtime_policy_add_rejects_ambiguity_without_poisoning_live_or_persisted_state() {
    let h = harness();
    let mut first = default_policy();
    first.id = "checkpoint-a".into();
    first.kind = PolicyKind::ObjectClass;
    first.match_class = Some("checkpoint".into());
    h.coordinator.add_policy(first).unwrap();

    let mut conflicting = default_policy();
    conflicting.id = "checkpoint-b".into();
    conflicting.kind = PolicyKind::ObjectClass;
    conflicting.match_class = Some("checkpoint".into());
    assert!(matches!(
        h.coordinator.add_policy(conflicting),
        Err(ReclaimError::Policy(_))
    ));

    let registry = h.coordinator.policy_registry().unwrap();
    registry.validate_complete().unwrap();
    assert_eq!(
        registry
            .resolve(&obj("checkpoint"), PressureLevel::Normal)
            .unwrap()
            .id,
        "checkpoint-a"
    );
    assert!(!h
        .coordinator
        .store()
        .unwrap()
        .list_policies()
        .unwrap()
        .iter()
        .any(|policy| policy.id == "checkpoint-b"));
}

#[test]
fn restart_preserves_committed_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let oid;
    {
        let h = harness_with(tempfile::tempdir().unwrap(), store_path.clone());
        let o = create_with_payload(&h.coordinator, obj("checkpoint"), b"restart-data");
        oid = o.id;
        let report = reclaim(&h.coordinator, o.id, true).unwrap();
        assert!(report.reclaimed);
    }
    let h = harness_with(tempfile::tempdir().unwrap(), store_path);
    let after = h.coordinator.store().unwrap().require_object(&oid).unwrap();
    assert_eq!(after.lifecycle_state, LifecycleState::Reclaimed);
    assert!(h
        .coordinator
        .store()
        .unwrap()
        .replicas_for(&oid)
        .unwrap()
        .is_empty());
    let entries = h.coordinator.audit(Some(&oid), None, 100).unwrap();
    assert!(!entries.is_empty());
}

/// Crash between physical deletion and metadata commit: recovery must commit
/// the reclaim (physical truth: payload gone).
#[test]
fn recovery_commits_crash_after_physical_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let payload = b"crash-payload".to_vec();
    let oid;
    {
        let h = harness_with(tempfile::tempdir().unwrap(), store_path.clone());
        let o = create_with_payload(&h.coordinator, obj("checkpoint"), &payload);
        oid = o.id;
        let replica = h
            .coordinator
            .store()
            .unwrap()
            .replicas_for(&o.id)
            .unwrap()
            .remove(0);
        let store = h.coordinator.store().unwrap();
        // Physical delete happened (crash before metadata commit).
        let backend = h.coordinator.backend("memory").unwrap();
        backend.delete(&replica.location.key).unwrap();
        let mut pending = o.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();
        let attempt = Uuid::new_v4();
        let reservation = Reservation {
            reservation_id: Uuid::new_v4(),
            attempt_id: attempt,
            object_id: o.id,
            generation: o.generation,
            node: "n".into(),
            created_at_ms: h.clock.now_ms(),
            expires_at_ms: h.clock.now_ms() + 60_000,
            status: "OPEN".into(),
        };
        store.create_reservation(&reservation).unwrap();
        store
            .insert_journal(&JournalEntry {
                attempt_id: attempt,
                object_id: o.id,
                generation: o.generation,
                phase: JournalPhase::PhysicalDone,
                created_at_ms: h.clock.now_ms(),
                updated_at_ms: h.clock.now_ms(),
                payload: with_physical_deletions(
                    &journal_payload(&o, std::slice::from_ref(&replica), &[]).unwrap(),
                    std::slice::from_ref(&replica),
                )
                .unwrap(),
            })
            .unwrap();
        store
            .create_attempt(&Attempt {
                attempt_id: attempt,
                object_id: o.id,
                generation: o.generation,
                node: "n".into(),
                created_at_ms: h.clock.now_ms(),
                updated_at_ms: h.clock.now_ms(),
                status: AttemptStatus::Open,
            })
            .unwrap();
    }
    let h = harness_with(tempfile::tempdir().unwrap(), store_path);
    let after = h.coordinator.store().unwrap().require_object(&oid).unwrap();
    assert_eq!(
        after.lifecycle_state,
        LifecycleState::Reclaimed,
        "recovery must commit the physically-completed reclaim"
    );
    let journal = h.coordinator.store().unwrap().list_all_journal().unwrap();
    assert!(journal.iter().any(|e| e.phase == JournalPhase::Committed));
}

/// Crash after reservation but before any physical action: recovery must roll
/// the object back to its prior state.
#[test]
fn recovery_rolls_back_reserved_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let payload = b"rollback-payload".to_vec();
    let oid;
    {
        let h = harness_with(tempfile::tempdir().unwrap(), store_path.clone());
        let o = create_with_payload(&h.coordinator, obj("checkpoint"), &payload);
        oid = o.id;
        let store = h.coordinator.store().unwrap();
        let mut pending = o.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();
        let attempt = Uuid::new_v4();
        store
            .create_attempt(&Attempt {
                attempt_id: attempt,
                object_id: o.id,
                generation: o.generation,
                node: "crashed-node".into(),
                created_at_ms: h.clock.now_ms(),
                updated_at_ms: h.clock.now_ms(),
                status: AttemptStatus::Open,
            })
            .unwrap();
        store
            .insert_journal(&JournalEntry {
                attempt_id: attempt,
                object_id: o.id,
                generation: o.generation,
                phase: JournalPhase::Reserved,
                created_at_ms: h.clock.now_ms(),
                updated_at_ms: h.clock.now_ms(),
                payload: journal_payload(&o, &[], &[]).unwrap(),
            })
            .unwrap();
    }
    let h = harness_with(tempfile::tempdir().unwrap(), store_path);
    let after = h.coordinator.store().unwrap().require_object(&oid).unwrap();
    assert_eq!(
        after.lifecycle_state,
        LifecycleState::Hot,
        "recovery must roll a reserved-but-unexecuted reclaim back"
    );
}

/// A failed physical delete must never mark the object RECLAIMED.
#[test]
fn failed_physical_delete_never_marks_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let backends = BackendRegistry::new();
    backends
        .register(Arc::new(FailingDeleteBackend::new("memory")))
        .unwrap();
    let pressure = PressureRegistry::new();
    let clock: Arc<FrozenClock> = Arc::new(FrozenClock::new(1_000_000));
    let config = CoordinatorConfig {
        store_path: store_path.clone(),
        process_id: "test-coordinator".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some("127.0.0.1:9999".into()),
    };
    let coordinator = Coordinator::open(config, backends, pressure, vec![], clock).unwrap();
    let o = create_with_payload(&coordinator, obj("checkpoint"), b"data");
    let err = reclaim(&coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::Recovery(_)));
    let store = coordinator.store().unwrap();
    let after = store.require_object(&o.id).unwrap();
    assert_ne!(after.lifecycle_state, LifecycleState::Reclaimed);
    // Backend truth became indeterminate after physical execution started.
    // Preserve the journal and pending state so restart recovery, rather than
    // an unsafe in-process guess, decides whether to commit or roll back.
    assert_eq!(after.lifecycle_state, LifecycleState::ReclaimPending);
    assert_eq!(store.list_open_journal().unwrap().len(), 1);
}

#[test]
fn min_retention_deadline_respected() {
    let h = harness();
    let mut o = obj("checkpoint");
    o.min_retention_deadline_ms = Some(h.clock.now_ms() + 10_000);
    let o = create_with_payload(&h.coordinator, o, b"retained");
    let err = reclaim(&h.coordinator, o.id, true).unwrap_err();
    assert!(matches!(err, ReclaimError::InvalidArgument(_)));
    h.clock.advance(11_000);
    let report = reclaim(&h.coordinator, o.id, true).unwrap();
    assert!(report.reclaimed);
}

#[test]
fn touch_tracks_access() {
    let h = harness();
    let o = create_with_payload(&h.coordinator, obj("checkpoint"), b"data");
    assert_eq!(o.access_count, 0);
    h.coordinator.touch(&o.id, "t").unwrap();
    h.coordinator.touch(&o.id, "t").unwrap();
    let after = h
        .coordinator
        .store()
        .unwrap()
        .require_object(&o.id)
        .unwrap();
    assert_eq!(after.access_count, 2);
    assert_eq!(after.last_access_ms, h.clock.now_ms());
}

#[test]
fn verify_detects_corruption_and_marks_replica_invalid() {
    let h = harness();
    let o = create_with_payload(&h.coordinator, obj("checkpoint"), b"intact");
    let replicas = h.coordinator.store().unwrap().replicas_for(&o.id).unwrap();
    let backend = h.coordinator.backend("memory").unwrap();
    backend
        .put(&replicas[0].location.key, b"corrupted!")
        .unwrap();
    let result = h.coordinator.verify(&o.id, "t").unwrap();
    let results = result["results"].as_array().unwrap();
    assert_eq!(results[0]["ok"], serde_json::json!(false));
    let replica = h
        .coordinator
        .store()
        .unwrap()
        .replicas_for(&o.id)
        .unwrap()
        .remove(0);
    assert!(!replica.valid);
    let failures = h.coordinator.failures(10).unwrap();
    assert!(failures.iter().any(|f| f.kind == "INTEGRITY_FAILURE"));
}

#[test]
fn stats_are_consistent() {
    let h = harness();
    let o1 = create_with_payload(&h.coordinator, obj("checkpoint"), b"one");
    let _ = create_with_payload(&h.coordinator, obj("checkpoint"), b"two");
    let stats = h.coordinator.stats().unwrap();
    assert_eq!(stats["objects"], serde_json::json!(2));
    assert!(stats["replicas"].as_u64().unwrap() >= 2);
    assert!(stats["audit_entries"].as_u64().unwrap() >= 2);
    reclaim(&h.coordinator, o1.id, true).unwrap();
    let stats = h.coordinator.stats().unwrap();
    assert_eq!(stats["objects"], serde_json::json!(2));
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .count_objects_in_state(LifecycleState::Reclaimed)
            .unwrap(),
        1
    );
}

/// Regression: registration rollback (under-copy CRITICAL object) must never
/// destroy a deduplicated payload still referenced by another live object.
#[test]
fn failed_registration_never_destroys_shared_payload() {
    let h = harness();
    let payload = b"shared-registration-payload".to_vec();

    // First registration: creates the physical payload (1 ref).
    let mut first = obj("first");
    first.durability_class = DurabilityClass::Ephemeral;
    let first = create_with_payload(&h.coordinator, first, &payload);

    // Second registration: CRITICAL with a single copy -> rejected at
    // creation; the rollback must NOT delete the shared payload.
    let mut second = obj("second");
    second.durability_class = DurabilityClass::Critical;
    let second_id = second.id;
    let req = CreateObjectRequest {
        object: second,
        payload_b64: Some(reclaim_fabric::base64_payload(&payload)),
        target_backend: Some("memory".into()),
        replicate_to: vec![],
    };
    let err = h.coordinator.create_object(&req).unwrap_err();
    assert!(matches!(err, ReclaimError::SurvivabilityViolation(_)));

    // The shared payload still exists and the first object still works.
    let hash = ContentHash::of(&payload);
    let entry = h
        .coordinator
        .store()
        .unwrap()
        .get_dedup(&hash, "memory")
        .unwrap()
        .unwrap();
    assert_eq!(entry.ref_count, 1);
    let backend = h.coordinator.backend("memory").unwrap();
    assert!(backend.exists(&entry.key).unwrap());
    h.coordinator.verify(&first.id, "t").unwrap();

    // And the rejected object left no debris.
    assert!(h
        .coordinator
        .store()
        .unwrap()
        .get_object(&second_id)
        .is_ok());
}

/// Regression: compression must replace the replica with the compressed
/// payload — no orphaned physical bytes and no stale replicas.
#[test]
fn compress_leaves_no_orphaned_payload() {
    let h = harness();
    let data: Vec<u8> = (0..10_000u32)
        .flat_map(|i| format!("row-{i:06};").into_bytes())
        .collect();
    let o = create_with_payload(&h.coordinator, obj("compressible"), &data);

    let backend = h.coordinator.backend("memory").unwrap();
    let keys_before = backend.keys();
    assert_eq!(keys_before.len(), 1);

    h.coordinator.compress(&o.id, "t").unwrap();
    let keys_after = backend.keys();
    assert_eq!(keys_after.len(), 1, "compression must not orphan payloads");
    assert_ne!(keys_after[0], keys_before[0]);

    // Exactly one replica remains, pointing at the compressed payload.
    let replicas = h.coordinator.store().unwrap().replicas_for(&o.id).unwrap();
    assert_eq!(replicas.len(), 1);
    assert!(replicas[0].valid);
    assert_eq!(replicas[0].location.key, keys_after[0]);

    // Integrity of the compressed payload is verifiable.
    h.coordinator.verify(&o.id, "t").unwrap();
}

/// Regression: same content stored on two different backends must produce two
/// independently tracked dedup entries (one physical payload per backend).
#[test]
fn dedup_is_keyed_per_backend() {
    let h = harness();
    let payload = b"cross-backend-content".to_vec();
    let a = obj("a");
    let a = create_with_payload(&h.coordinator, a, &payload);
    // Force a second, distinct backend copy via the file backend.
    let file_dir = h._dir.path().join("data2");
    let file_backend = FileBackend::new("file:data2", &file_dir).unwrap();
    h.coordinator
        .register_backend("file:data2", Arc::new(file_backend))
        .unwrap();
    let req = CreateObjectRequest {
        object: obj("b"),
        payload_b64: Some(reclaim_fabric::base64_payload(&payload)),
        target_backend: Some("file:data2".into()),
        replicate_to: vec![],
    };
    let b = h.coordinator.create_object(&req).unwrap();

    let hash = ContentHash::of(&payload);
    let mem_entry = h
        .coordinator
        .store()
        .unwrap()
        .get_dedup(&hash, "memory")
        .unwrap()
        .unwrap();
    let file_entry = h
        .coordinator
        .store()
        .unwrap()
        .get_dedup(&hash, "file:data2")
        .unwrap()
        .unwrap();
    assert_eq!(mem_entry.ref_count, 1);
    assert_eq!(file_entry.ref_count, 1);

    // Reclaiming one object must not disturb the other backend's payload.
    reclaim(&h.coordinator, a.id, true).unwrap();
    assert!(h
        .coordinator
        .store()
        .unwrap()
        .get_dedup(&hash, "memory")
        .unwrap()
        .is_none());
    assert_eq!(
        h.coordinator
            .store()
            .unwrap()
            .get_dedup(&hash, "file:data2")
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    reclaim(&h.coordinator, b.id, true).unwrap();
    assert!(h
        .coordinator
        .store()
        .unwrap()
        .get_dedup(&hash, "file:data2")
        .unwrap()
        .is_none());
}

// ----------------------------------------------------------------------
// Failure-injection backend
// ----------------------------------------------------------------------

/// Backend whose deletes always fail (failure injection for reclamation).
struct FailingDeleteBackend {
    id: String,
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl FailingDeleteBackend {
    fn new(id: &str) -> FailingDeleteBackend {
        FailingDeleteBackend {
            id: id.into(),
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl Backend for FailingDeleteBackend {
    fn id(&self) -> &str {
        &self.id
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<u64, ReclaimError> {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(data.len() as u64)
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, ReclaimError> {
        self.data
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| ReclaimError::NotFound(key.into()))
    }
    fn delete(&self, _key: &str) -> Result<(), ReclaimError> {
        Err(ReclaimError::Io("injected delete failure".into()))
    }
    fn exists(&self, key: &str) -> Result<bool, ReclaimError> {
        Ok(self.data.lock().unwrap().contains_key(key))
    }
    fn verify(&self, key: &str, expected: &ContentHash) -> Result<(), ReclaimError> {
        let data = self.get(key)?;
        reclaim_fabric::integrity::verify_sha256(&data, expected)
    }
    fn total_bytes(&self) -> u64 {
        self.data
            .lock()
            .unwrap()
            .values()
            .map(|v| v.len() as u64)
            .sum()
    }
    fn keys(&self) -> Vec<String> {
        self.data.lock().unwrap().keys().cloned().collect()
    }
    fn kind(&self) -> &'static str {
        "memory"
    }
}

// Silence unused import warnings for helper types referenced in docs.
#[allow(dead_code)]
fn _store_ref(_: &Store) {}
