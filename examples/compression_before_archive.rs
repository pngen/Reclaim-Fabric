//! Example 9: compression before archive.
//!
//! Compress the payload, verify the round trip, then archive it durably.
//! Run with: cargo run --example compression_before_archive

mod common;

use common::{checkpoint, create_with_payload, harness};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness();
    let coordinator = &h.coordinator;

    let data: Vec<u8> = (0..50_000u32)
        .flat_map(|i| format!("checkpoint-row-{i:08};").into_bytes())
        .collect();
    let obj = create_with_payload(coordinator, checkpoint("checkpoint"), &data)?;

    let result = coordinator.compress(&obj.id, "example")?;
    println!(
        "compressed {} -> {} bytes (ratio {:.3}, codec {})",
        result.original_size, result.compressed_size, result.ratio, result.codec
    );
    assert!(result.compressed_size < result.original_size);

    // Archive only after integrity is verified.
    let record = coordinator.archive(&obj.id, "example")?;
    println!("archived: {} ({} bytes)", record.archive_id, record.size);

    // Restore back to a hot backend.
    let restored = coordinator.restore(&obj.id, "example")?;
    println!("restored to state {:?}", restored.lifecycle_state);
    coordinator.verify(&obj.id, "example")?;
    println!("restored payload verified");
    Ok(())
}
