//! Example 11: crash/restart recovery.
//!
//! Simulate a crash between the physical deletion and the metadata commit
//! of a reclaim, restart the runtime, and watch recovery reconcile state.
//! Run with: cargo run --example crash_restart

mod common;

use common::{checkpoint, create_with_payload};
use reclaim_fabric::persistence::{
    Attempt, AttemptStatus, JournalEntry, JournalPhase, Reservation,
};
use reclaim_fabric::recovery::{journal_payload, with_physical_deletions};
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.db").to_str().unwrap().to_string();
    let payload = b"crash-recovery-payload".to_vec();
    let oid;

    {
        // Phase 1: run a coordinator, create state, then simulate a crash at
        // the most dangerous point: after the physical delete, before the
        // metadata commit (journal phase PHYSICAL_DONE, object stuck in
        // RECLAIM_PENDING).
        let h = harness_with_store(store_path.clone());
        let coordinator = &h.coordinator;
        let o = create_with_payload(coordinator, checkpoint("checkpoint"), &payload)?;
        oid = o.id;
        let replica = coordinator.store()?.replicas_for(&o.id)?.remove(0);
        let backend = coordinator.backend("memory")?;
        backend.delete(&replica.location.key)?; // physical deletion happened

        let store = coordinator.store()?;
        let mut pending = o.clone();
        pending.lifecycle_state = reclaim_fabric::lifecycle::LifecycleState::ReclaimPending;
        store.update_object(&pending)?;
        let attempt = Uuid::new_v4();
        store.create_reservation(&Reservation {
            reservation_id: Uuid::new_v4(),
            attempt_id: attempt,
            object_id: o.id,
            generation: o.generation,
            node: "crashed-node".into(),
            created_at_ms: 1_000_000,
            expires_at_ms: 1_000_000 + 60_000,
            status: "OPEN".into(),
        })?;
        store.insert_journal(&JournalEntry {
            attempt_id: attempt,
            object_id: o.id,
            generation: o.generation,
            phase: JournalPhase::PhysicalDone,
            created_at_ms: 1_000_000,
            updated_at_ms: 1_000_000,
            payload: with_physical_deletions(
                &journal_payload(&o, std::slice::from_ref(&replica), &[])?,
                std::slice::from_ref(&replica),
            )?,
        })?;
        store.create_attempt(&Attempt {
            attempt_id: attempt,
            object_id: o.id,
            generation: o.generation,
            node: "crashed-node".into(),
            created_at_ms: 1_000_000,
            updated_at_ms: 1_000_000,
            status: AttemptStatus::Open,
        })?;
        println!("simulated crash after physical delete, before metadata commit");
    }

    {
        // Phase 2: restart the coordinator; recovery reconciles the journal.
        let h = harness_with_store(store_path);
        let coordinator = &h.coordinator;
        let report = coordinator.recover()?;
        println!(
            "recovery: committed={:?} rolled_back={:?} errors={:?}",
            report.committed, report.rolled_back, report.errors
        );
        let after = coordinator.store()?.require_object(&oid)?;
        println!("object state after recovery: {:?}", after.lifecycle_state);
        assert_eq!(
            after.lifecycle_state,
            reclaim_fabric::lifecycle::LifecycleState::Reclaimed,
            "physical truth (payload gone) must win"
        );
    }
    Ok(())
}

fn harness_with_store(store_path: String) -> common::Harness {
    let dir = tempfile::tempdir().unwrap();
    let backends = reclaim_fabric::backends::BackendRegistry::new();
    backends
        .register(std::sync::Arc::new(
            reclaim_fabric::backends::MemoryBackend::new("memory"),
        ))
        .unwrap();
    let pressure = reclaim_fabric::pressure::PressureRegistry::new();
    let config = reclaim_fabric::coordinator::CoordinatorConfig {
        store_path,
        process_id: "crash-example".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some("127.0.0.1:9999".into()),
    };
    let coordinator = reclaim_fabric::coordinator::Coordinator::open(
        config,
        backends,
        pressure,
        vec![],
        std::sync::Arc::new(reclaim_fabric::coordinator::SystemClock),
    )
    .unwrap();
    common::Harness {
        _dir: dir,
        coordinator,
    }
}
