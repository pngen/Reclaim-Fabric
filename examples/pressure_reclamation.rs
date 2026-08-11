//! Example 2: pressure-driven reclamation.
//!
//! Under NORMAL pressure an object is retained; under CRITICAL pressure the
//! emergency policy drives reclamation of marginal state.
//! Run with: cargo run --example pressure_reclamation

mod common;

use common::{checkpoint, create_with_payload, harness};
use reclaim_fabric::pressure::PressureLevel;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;
    coordinator.register_synthetic_pressure("synthetic")?;

    // Marginal state: some reuse value, moderate recompute cost.
    let mut obj = checkpoint("marginal");
    obj.reuse_probability = 0.2;
    obj.recompute_cost = Some(50.0);
    obj.memory_cost_per_byte_sec = 0.8;
    let obj = create_with_payload(coordinator, obj, b"marginal-state")?;

    coordinator.set_pressure("synthetic", PressureLevel::Normal)?;
    let normal = coordinator.plan(&obj.id, "example")?;
    println!("NORMAL pressure verdict: {:?}", normal.decision.verdict);

    coordinator.set_pressure("synthetic", PressureLevel::Critical)?;
    let critical = coordinator.plan(&obj.id, "example")?;
    println!("CRITICAL pressure verdict: {:?}", critical.decision.verdict);
    println!("emergency policy: {}", critical.decision.policy_id);

    // At CRITICAL pressure the object is a candidate.
    let candidates = coordinator.candidates(10, "example")?;
    assert!(candidates.iter().any(|c| c.decision.object_id == obj.id));

    // Protected state still cannot be reclaimed, even by the emergency policy.
    coordinator.set_protected(&obj.id, true, "example")?;
    coordinator.set_pressure("synthetic", PressureLevel::Critical)?;
    let decision = coordinator.plan(&obj.id, "example")?;
    assert_eq!(
        decision.decision.verdict,
        reclaim_fabric::economics::ReclaimVerdict::Retain
    );
    println!("protected invariant holds under CRITICAL pressure: RETAIN");
    Ok(())
}
