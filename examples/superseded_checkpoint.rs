//! Example 7: superseded checkpoint reclamation.
//!
//! checkpoint_generation_11 supersedes checkpoint_generation_10; once
//! generation 10 is no longer required, it becomes reclaimable.
//! Run with: cargo run --example superseded_checkpoint

mod common;

use common::{checkpoint, create_with_payload, harness, reclaim};
use reclaim_fabric::lineage::EdgeKind;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;

    let v10 = create_with_payload(coordinator, checkpoint("checkpoint"), b"gen-10")?;
    let v11 = create_with_payload(coordinator, checkpoint("checkpoint"), b"gen-11")?;
    coordinator.add_lineage(v10.id, v11.id, EdgeKind::Supersedes, "example")?;

    // Generation 10 is superseded; its payload is redundant.
    let graph = coordinator.lineage()?;
    let superseded = graph.superseded(&[v10.id, v11.id].into_iter().collect());
    println!("superseded: {}", superseded.contains(&v10.id));

    let report = reclaim(coordinator, v10.id, true)?;
    assert!(report.reclaimed);
    println!("generation 10 reclaimed, generation 11 untouched");
    assert_eq!(
        coordinator
            .store()?
            .require_object(&v11.id)?
            .lifecycle_state,
        reclaim_fabric::lifecycle::LifecycleState::Hot
    );
    Ok(())
}
