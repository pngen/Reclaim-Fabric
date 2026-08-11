//! Example 5: pinned object protection.
//!
//! Pinning is a hard invariant: neither automatic policy nor --force can
//! reclaim pinned state. Run with: cargo run --example pinned_object

mod common;

use common::{checkpoint, create_with_payload, harness, reclaim};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;
    let mut obj = checkpoint("critical-buffer");
    obj.reuse_probability = 0.0;
    obj.recompute_cost = Some(0.01); // cheap to rebuild, but pinned anyway
    let obj = create_with_payload(coordinator, obj, b"pinned-buffer")?;

    coordinator.pin(&obj.id, "example")?;
    let err = reclaim(coordinator, obj.id, true).unwrap_err();
    println!("forced reclaim of pinned object: {err}");

    // Even the decision engine refuses to recommend reclaim.
    let decision = coordinator.plan(&obj.id, "example")?;
    assert_eq!(
        decision.decision.verdict,
        reclaim_fabric::economics::ReclaimVerdict::Retain
    );
    println!("pinned invariant holds");

    // Unpinning makes the state reclaimable again.
    coordinator.unpin(&obj.id, "example")?;
    let report = reclaim(coordinator, obj.id, true)?;
    assert!(report.reclaimed);
    println!("after unpin: reclaimed");
    Ok(())
}
