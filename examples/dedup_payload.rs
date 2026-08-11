//! Example 8: deduplicated payload handling.
//!
//! Two logical objects share one physical payload via content identity.
//! Reclaiming one must never destroy the shared payload.
//! Run with: cargo run --example dedup_payload

mod common;

use common::{checkpoint, create_with_payload, harness, reclaim};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;

    let payload = b"the-same-model-weights".to_vec();
    let a = create_with_payload(coordinator, checkpoint("weights"), &payload)?;
    let b = create_with_payload(coordinator, checkpoint("weights"), &payload)?;

    let hash = reclaim_fabric::integrity::ContentHash::of(&payload);
    let entry = coordinator
        .store()?
        .get_dedup(&hash, "memory")?
        .expect("dedup entry");
    println!(
        "shared payload: ref_count={} key={}",
        entry.ref_count, entry.key
    );
    assert_eq!(entry.ref_count, 2);

    let backend = coordinator.backend("memory")?;
    assert!(backend.exists(&entry.key)?);

    // Reclaim one object: the physical payload must survive.
    let report = reclaim(coordinator, a.id, true)?;
    assert!(report.reclaimed);
    assert!(backend.exists(&entry.key)?, "shared payload must survive");
    println!(
        "payload survives first reclaim (ref_count now {})",
        coordinator
            .store()?
            .get_dedup(&hash, "memory")?
            .unwrap()
            .ref_count
    );

    // Reclaim the last reference: the payload is finally released.
    reclaim(coordinator, b.id, true)?;
    assert!(
        !backend.exists(&entry.key)?,
        "payload released with the last reference"
    );
    println!("payload released with the last reference");
    Ok(())
}
