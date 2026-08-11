//! Example 1: basic object lifecycle.
//!
//! Register a state object, track accesses, plan, and reclaim it.
//! Run with: cargo run --example basic_lifecycle

mod common;

use common::{checkpoint, create_with_payload, harness, reclaim};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;

    // Register reusable state (e.g. a recomputation checkpoint).
    let mut obj = checkpoint("checkpoint");
    obj.reuse_probability = 0.01;
    obj.recompute_cost = Some(1.0); // cheap to rebuild
    let obj = create_with_payload(coordinator, obj, b"model-state-v1")?;
    println!("created {} state={:?}", obj.id, obj.lifecycle_state);

    // Track accesses.
    coordinator.touch(&obj.id, "example")?;
    coordinator.touch(&obj.id, "example")?;

    // Deterministic decision with a replayable explanation.
    let decision = coordinator.plan(&obj.id, "example")?;
    println!(
        "decision: {:?} score={:.2}",
        decision.decision.verdict, decision.decision.score
    );
    println!(
        "explanation: {}",
        serde_json::to_string_pretty(&decision.explanation)?
    );

    // Transactional reclaim: plan -> reserve -> validate -> execute -> verify -> commit.
    let report = reclaim(coordinator, obj.id, false)?;
    println!(
        "reclaimed: {} ({} -> {})",
        report.reclaimed, report.prior_state, report.final_state
    );

    let after = coordinator.store()?.require_object(&obj.id)?;
    assert_eq!(
        after.lifecycle_state,
        reclaim_fabric::lifecycle::LifecycleState::Reclaimed
    );
    assert!(coordinator.store()?.replicas_for(&obj.id)?.is_empty());

    // The audit trail records the whole lifecycle.
    let audit = coordinator.audit(Some(&obj.id), None, 100)?;
    println!("audit entries: {}", audit.len());
    Ok(())
}
