//! Example 4: cheap recomputation is reclaimed.
//!
//! Rebuilding costs almost nothing, so the runtime reaps the state.
//! Run with: cargo run --example cheap_recompute_reclaimed

mod common;

use common::{checkpoint, create_with_payload, harness};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;
    let mut obj = checkpoint("cheap");
    obj.reuse_probability = 0.001;
    obj.recompute_cost = Some(0.01);
    let obj = create_with_payload(coordinator, obj, b"rebuildable-state")?;

    let decision = coordinator.plan(&obj.id, "example")?;
    println!("verdict: {:?}", decision.decision.verdict);
    for reason in &decision.decision.reasons {
        println!("  reason: {reason}");
    }
    let report = common::reclaim(coordinator, obj.id, false)?;
    assert!(report.reclaimed);
    println!("cheap state reclaimed");
    Ok(())
}
