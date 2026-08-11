//! Example 12: policy-driven behavior.
//!
//! Register a class-specific policy and an owner policy; show that the
//! engine resolves by specificity and records the exact policy version.
//! Run with: cargo run --example policy_driven

mod common;

use common::{checkpoint, create_with_payload, harness};
use reclaim_fabric::economics::CostWeights;
use reclaim_fabric::policy::{Policy, PolicyKind};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;

    // Class policy: "checkpoint" objects are valuable, never reclaim below
    // a high reuse bar.
    coordinator.add_policy(Policy {
        id: "reclaim-checkpoints".into(),
        version: "v1".into(),
        kind: PolicyKind::ObjectClass,
        reclaim_threshold: 10_000.0,
        min_reuse_probability: 0.8,
        weights: CostWeights::default(),
        match_class: Some("checkpoint".into()),
        match_owner: None,
        match_pressure: None,
        match_durability: None,
        match_survivability: None,
        emergency: false,
        description: "checkpoint states are expensive to lose".into(),
    })?;

    // Owner policy: this owner's scratch state is always cheap to rebuild.
    coordinator.add_policy(Policy {
        id: "reclaim-scratch".into(),
        version: "v2".into(),
        kind: PolicyKind::Owner,
        reclaim_threshold: 0.0,
        min_reuse_probability: 1.0,
        weights: CostWeights::default(),
        match_class: None,
        match_owner: Some("scratch-owner".into()),
        match_pressure: None,
        match_durability: None,
        match_survivability: None,
        emergency: false,
        description: "scratch state from this owner is aggressively reclaimable".into(),
    })?;

    let mut checkpoint_obj = checkpoint("checkpoint");
    checkpoint_obj.reuse_probability = 0.01;
    checkpoint_obj.recompute_cost = Some(1.0);
    let checkpoint_obj = create_with_payload(coordinator, checkpoint_obj, b"cp")?;
    let d1 = coordinator.plan(&checkpoint_obj.id, "example")?;
    println!(
        "checkpoint object -> policy {} (verdict {:?})",
        d1.decision.policy_id, d1.decision.verdict
    );
    assert_eq!(d1.decision.policy_id, "reclaim-checkpoints");
    assert_eq!(d1.decision.policy_version, "v1");
    assert_eq!(
        d1.decision.verdict,
        reclaim_fabric::economics::ReclaimVerdict::Retain
    );

    let mut scratch = checkpoint("scratch");
    scratch.class = "scratch".into();
    scratch.owner = "scratch-owner".into();
    scratch.reuse_probability = 0.5;
    scratch.recompute_cost = Some(1.0);
    let scratch = create_with_payload(coordinator, scratch, b"scratch")?;
    let d2 = coordinator.plan(&scratch.id, "example")?;
    println!(
        "scratch object   -> policy {} (verdict {:?})",
        d2.decision.policy_id, d2.decision.verdict
    );
    assert_eq!(d2.decision.policy_id, "reclaim-scratch");

    // The policy list shows every registered versioned policy.
    let registry = coordinator.policy_registry()?;
    for p in registry.list() {
        println!("registered: {} (kind {:?})", p.full_id(), p.kind);
    }
    Ok(())
}
