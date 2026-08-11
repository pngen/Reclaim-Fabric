//! Example 3: expensive recomputation is retained.
//!
//! Rebuilding this state costs 1e9 units; keeping it is cheaper.
//! Run with: cargo run --example expensive_recompute_retained

mod common;

use common::{checkpoint, create_with_payload, harness};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;
    let mut obj = checkpoint("expensive");
    obj.reuse_probability = 0.9;
    obj.recompute_cost = Some(1e9);
    let obj = create_with_payload(coordinator, obj, b"irreplaceable-state")?;

    let decision = coordinator.plan(&obj.id, "example")?;
    println!("verdict: {:?}", decision.decision.verdict);
    println!("score:   {:.4}", decision.decision.score);

    // A reclaim request without force leaves the object untouched.
    let report = common::reclaim(coordinator, obj.id, false)?;
    assert!(!report.reclaimed);
    assert_eq!(
        coordinator
            .store()?
            .require_object(&obj.id)?
            .lifecycle_state,
        reclaim_fabric::lifecycle::LifecycleState::Hot
    );
    println!("expensive state retained (RETAIN)");
    Ok(())
}
