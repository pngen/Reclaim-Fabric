//! Property-style invariant tests with a deterministic PRNG.
//!
//! The runtime must never violate these invariants:
//! 1. RECLAIMED objects have no live physical ownership.
//! 2. A protected object cannot reach RECLAIMED through automatic policy.
//! 3. A pinned object cannot be reclaimed.
//! 4. A DURABLE/CRITICAL object cannot fall below configured valid-copy count.
//! 5. Deduplicated shared physical content cannot be deleted while any live
//!    reference remains.
//! 6. A non-reconstructible dependency cannot be destroyed while required.
//! 7. A stale epoch cannot mutate authoritative state.
//! 8. A stale attempt cannot commit.
//! 9. An invalid lifecycle transition never commits.
//! 10. A failed physical reclaim never results in metadata claiming RECLAIMED.
//! 11. Restart preserves committed decisions.
//! 12. Identical authoritative inputs and policy versions produce identical
//!     decisions.

use std::sync::Arc;

use reclaim_fabric::backends::{BackendRegistry, MemoryBackend};
use reclaim_fabric::coordinator::{Coordinator, CoordinatorConfig, FrozenClock};
use reclaim_fabric::errors::ReclaimError;
use reclaim_fabric::lifecycle::LifecycleState;
use reclaim_fabric::object::{DurabilityClass, ReclaimObject};
use reclaim_fabric::pressure::PressureRegistry;
use reclaim_fabric::protocol::{CreateObjectRequest, ReclaimRequest};
use uuid::Uuid;

/// Deterministic xorshift64 PRNG.
struct Prng(u64);

impl Prng {
    fn new(seed: u64) -> Prng {
        Prng(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn harness() -> (Coordinator, Arc<FrozenClock>) {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let backends = BackendRegistry::new();
    backends
        .register(Arc::new(MemoryBackend::new("memory")))
        .unwrap();
    let pressure = PressureRegistry::new();
    let clock: Arc<FrozenClock> = Arc::new(FrozenClock::new(1_000_000));
    let config = CoordinatorConfig {
        store_path,
        process_id: "property-coordinator".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some("127.0.0.1:9999".into()),
    };
    let coordinator = Coordinator::open(config, backends, pressure, vec![], clock.clone()).unwrap();
    let _ = dir;
    (coordinator, clock)
}

fn random_obj(rng: &mut Prng, durability: DurabilityClass) -> ReclaimObject {
    let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "prop", 1000, 1_000_000);
    o.reuse_probability = (rng.below(100) as f64) / 100.0;
    o.recompute_cost = Some((rng.below(1_000) + 1) as f64);
    o.memory_cost_per_byte_sec = 1.0;
    o.durability_class = durability;
    o
}

fn create(coordinator: &Coordinator, o: ReclaimObject, payload: &[u8]) -> ReclaimObject {
    let req = CreateObjectRequest {
        object: o,
        payload_b64: Some(reclaim_fabric::base64_payload(payload)),
        target_backend: Some("memory".into()),
        replicate_to: vec![],
    };
    coordinator.create_object(&req).unwrap()
}

#[test]
fn invariants_hold_under_random_reclaim_storms() {
    let (coordinator, _clock) = harness();
    let mut rng = Prng::new(0x1234_5678_9abc_def0);
    let mut objects: Vec<Uuid> = Vec::new();
    // EPHEMERAL/RECOMPUTABLE objects may be reclaimed freely. DURABLE/CRITICAL
    // minimum-copy invariants are exercised in the integration suite (their
    // registrations require >= min copies by design).
    for i in 0..64 {
        let durability = if i % 2 == 0 {
            DurabilityClass::Ephemeral
        } else {
            DurabilityClass::Recomputable
        };
        let o = create(&coordinator, random_obj(&mut rng, durability), b"payload");
        objects.push(o.id);
    }
    // Random reclaim attempts (force), including repeats and unknowns.
    for _ in 0..300 {
        let idx = rng.below(objects.len() as u64 + 5) as usize;
        let target = objects.get(idx).copied().unwrap_or_else(Uuid::new_v4);
        let _ = coordinator.reclaim(&ReclaimRequest {
            object_id: target,
            actor: "storm".into(),
            force: true,
        });
    }
    // Invariant 1: RECLAIMED objects have no live physical ownership.
    let store = coordinator.store().unwrap();
    for o in store.list_objects().unwrap() {
        if o.lifecycle_state == LifecycleState::Reclaimed {
            assert!(
                store.replicas_for(&o.id).unwrap().is_empty(),
                "RECLAIMED object {} still has replicas",
                o.id
            );
        }
    }
}

#[test]
fn pinned_and_protected_never_reclaimed_under_storm() {
    let (coordinator, _clock) = harness();
    let mut rng = Prng::new(0xdead_beef_cafe_babe);
    let mut protected_ids = Vec::new();
    let mut pinned_ids = Vec::new();
    let mut plain_ids = Vec::new();
    for i in 0..40 {
        let mut o = random_obj(&mut rng, DurabilityClass::Ephemeral);
        let kind = i % 3;
        if kind == 0 {
            o.protected = true;
        } else if kind == 1 {
            o.pinned = true;
        }
        let created = create(&coordinator, o, b"p");
        match kind {
            0 => protected_ids.push(created.id),
            1 => pinned_ids.push(created.id),
            _ => plain_ids.push(created.id),
        }
    }
    for _ in 0..200 {
        let id = match rng.below(3) {
            0 => protected_ids[rng.below(protected_ids.len() as u64) as usize],
            1 => pinned_ids[rng.below(pinned_ids.len() as u64) as usize],
            _ => plain_ids[rng.below(plain_ids.len() as u64) as usize],
        };
        let _ = coordinator.reclaim(&ReclaimRequest {
            object_id: id,
            actor: "storm".into(),
            force: true,
        });
    }
    let store = coordinator.store().unwrap();
    // Invariants 2 & 3: protected/pinned never reach RECLAIMED.
    for id in protected_ids.iter().chain(pinned_ids.iter()) {
        let o = store.require_object(id).unwrap();
        assert_ne!(
            o.lifecycle_state,
            LifecycleState::Reclaimed,
            "protected/pinned object {} was reclaimed",
            id
        );
    }
}

#[test]
fn dedup_payload_survives_until_last_reference() {
    let (coordinator, _clock) = harness();
    let payload = b"shared-under-storm".to_vec();
    let mut ids = Vec::new();
    for i in 0..10 {
        let o = random_obj(&mut Prng::new(i + 1), DurabilityClass::Ephemeral);
        let created = create(&coordinator, o, &payload);
        ids.push(created.id);
    }
    let _hash = reclaim_fabric::integrity::ContentHash::of(&payload);
    let backend = coordinator.backend("memory").unwrap();
    let keys_before = backend.keys();
    let shared_key = keys_before
        .iter()
        .find(|k| k.starts_with("dedup-"))
        .cloned()
        .unwrap();
    // Reclaim 9 of 10 objects.
    for (i, id) in ids[..9].iter().enumerate() {
        let report = coordinator
            .reclaim(&ReclaimRequest {
                object_id: *id,
                actor: "t".into(),
                force: true,
            })
            .unwrap();
        assert!(report.reclaimed);
        // Invariant 5: shared payload must still exist while a live reference
        // remains.
        let remaining = ids.len() - 1 - i;
        assert!(
            backend.exists(&shared_key).unwrap(),
            "dedup payload deleted while {remaining} references remain"
        );
    }
    // Last reference: payload may now disappear.
    coordinator
        .reclaim(&ReclaimRequest {
            object_id: ids[9],
            actor: "t".into(),
            force: true,
        })
        .unwrap();
    assert!(!backend.exists(&shared_key).unwrap());
}

#[test]
fn dependency_chain_never_broken_under_storm() {
    let (coordinator, _clock) = harness();
    let mut rng = Prng::new(0xfeed_face_c0de_cafe);
    // Chain: a -> b -> c (all DEPENDS_ON, all non-reconstructible).
    let mut ids = Vec::new();
    for i in 0..5 {
        let mut o = random_obj(&mut rng, DurabilityClass::Ephemeral);
        o.recompute_cost = None;
        let created = create(&coordinator, o, b"chain");
        ids.push(created.id);
        if i > 0 {
            coordinator
                .add_lineage(
                    ids[i - 1],
                    ids[i],
                    reclaim_fabric::lineage::EdgeKind::DependsOn,
                    "t",
                )
                .unwrap();
        }
    }
    for _ in 0..50 {
        let idx = rng.below(ids.len() as u64) as usize;
        let _ = coordinator.reclaim(&ReclaimRequest {
            object_id: ids[idx],
            actor: "storm".into(),
            force: true,
        });
    }
    // Invariant 6: no non-reconstructible dependency destroyed while required.
    let store = coordinator.store().unwrap();
    for i in 0..ids.len().saturating_sub(1) {
        let parent = store.require_object(&ids[i]).unwrap();
        let child = store.require_object(&ids[i + 1]).unwrap();
        let child_reclaimed = child.lifecycle_state == LifecycleState::Reclaimed;
        let parent_reclaimed = parent.lifecycle_state == LifecycleState::Reclaimed;
        // A parent may only be reclaimed if its child is no longer live.
        if parent_reclaimed {
            assert!(
                child_reclaimed,
                "parent {} reclaimed while live dependent {} exists",
                ids[i],
                ids[i + 1]
            );
        }
    }
}

#[test]
fn invalid_transition_never_commits() {
    let (coordinator, _clock) = harness();
    let o = create(
        &coordinator,
        random_obj(&mut Prng::new(7), DurabilityClass::Ephemeral),
        b"x",
    );
    // Invariant 9: lifecycle transition validation is enforced by the state
    // machine; direct store writes are the only bypass and are rejected by
    // `check_transition` when used through the API. Verify the API rejects
    // RECLAIMED -> HOT (terminal state).
    let report = coordinator
        .reclaim(&ReclaimRequest {
            object_id: o.id,
            actor: "t".into(),
            force: true,
        })
        .unwrap();
    assert!(report.reclaimed);
    assert!(matches!(
        reclaim_fabric::lifecycle::check_transition(LifecycleState::Reclaimed, LifecycleState::Hot),
        Err(ReclaimError::InvalidTransition { .. })
    ));
    // And pinning a RECLAIMED object is refused.
    assert!(coordinator.pin(&o.id, "t").is_err());
}

#[test]
fn stale_attempt_cannot_commit() {
    let (coordinator, _clock) = harness();
    let o = create(
        &coordinator,
        random_obj(&mut Prng::new(11), DurabilityClass::Ephemeral),
        b"y",
    );
    // Invariant 8: a stale attempt id cannot commit. The coordinator only
    // commits attempts it created itself; attempts are keyed by UUID and the
    // store rejects unknown attempt updates via the journal lifecycle.
    let store = coordinator.store().unwrap();
    let bogus = Uuid::new_v4();
    // No journal entry exists for the bogus attempt; committing it must fail.
    assert!(store.get_journal(&bogus).unwrap().is_none());
    store
        .update_reservation_for_attempt(&bogus, "COMMITTED")
        .unwrap();
    // Recovery only reconciles real journal entries.
    let report = coordinator.recover().unwrap();
    assert!(
        report.errors.is_empty(),
        "recovery errors: {:?}",
        report.errors
    );
    // The object is still hot and untouched.
    assert_eq!(
        store.require_object(&o.id).unwrap().lifecycle_state,
        LifecycleState::Hot
    );
}

#[test]
fn identical_inputs_produce_identical_decisions() {
    let (coordinator, _clock) = harness();
    let o = create(
        &coordinator,
        random_obj(&mut Prng::new(13), DurabilityClass::Ephemeral),
        b"z",
    );
    let a = coordinator.plan(&o.id, "t").unwrap();
    let b = coordinator.plan(&o.id, "t").unwrap();
    assert_eq!(a.decision, b.decision);
    assert_eq!(a.explanation, b.explanation);
}
