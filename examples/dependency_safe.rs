//! Example 6: dependency-safe reclamation.
//!
//! A parent cannot be reclaimed while a non-reconstructible dependent
//! requires it. Run with: cargo run --example dependency_safe

mod common;

use common::{checkpoint, create_with_payload, harness, reclaim};
use reclaim_fabric::lineage::EdgeKind;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;

    let parent = create_with_payload(coordinator, checkpoint("parent"), b"parent-state")?;
    let mut child = checkpoint("child");
    child.recompute_cost = None; // non-reconstructible: the child needs its parent
    let child = create_with_payload(coordinator, child, b"child-state")?;
    coordinator.add_lineage(parent.id, child.id, EdgeKind::DependsOn, "example")?;

    let err = reclaim(coordinator, parent.id, true).unwrap_err();
    println!("parent reclaim blocked: {err}");

    // A reconstructible dependent does not block reclamation.
    let mut child2 = checkpoint("child2");
    child2.recompute_cost = Some(5.0);
    let child2 = create_with_payload(coordinator, child2, b"child2-state")?;
    coordinator.add_lineage(parent.id, child2.id, EdgeKind::DependsOn, "example")?;
    coordinator.remove_lineage(parent.id, child.id, EdgeKind::DependsOn, "example")?;
    let report = reclaim(coordinator, parent.id, true)?;
    println!("parent reclaimed: {}", report.reclaimed);
    let _ = child;
    let _ = child2;
    Ok(())
}
