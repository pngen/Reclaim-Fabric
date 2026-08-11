//! Candidate selection benchmarks at 1K / 10K / 100K object counts.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use reclaim_fabric::backends::{BackendRegistry, MemoryBackend};
use reclaim_fabric::coordinator::{Coordinator, CoordinatorConfig, SystemClock};
use reclaim_fabric::object::ReclaimObject;
use reclaim_fabric::pressure::PressureRegistry;
use reclaim_fabric::protocol::CreateObjectRequest;
use uuid::Uuid;

fn open_coordinator() -> Coordinator {
    let backends = BackendRegistry::new();
    backends
        .register(Arc::new(MemoryBackend::new("memory")))
        .unwrap();
    let pressure = PressureRegistry::new();
    let config = CoordinatorConfig {
        store_path: ":memory:".into(),
        process_id: "bench-coordinator".into(),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some("127.0.0.1:9999".into()),
    };
    Coordinator::open(config, backends, pressure, vec![], Arc::new(SystemClock)).unwrap()
}

fn sample_object(id: Uuid) -> ReclaimObject {
    let mut o = ReclaimObject::new(id, 0, "bench", 4096, 0);
    o.reuse_probability = 0.01;
    o.recompute_cost = Some(1.0);
    o.memory_cost_per_byte_sec = 1.0;
    o
}

fn populate(coordinator: &Coordinator, count: usize) {
    for _ in 0..count {
        let req = CreateObjectRequest {
            object: sample_object(Uuid::new_v4()),
            payload_b64: None,
            target_backend: None,
            replicate_to: vec![],
        };
        let _ = coordinator.create_object(&req).unwrap();
    }
}

fn bench_candidate_selection(c: &mut Criterion) {
    let mut group = c.benchmark_group("candidate_selection");

    for count in [1_000usize, 10_000, 100_000] {
        let coordinator = open_coordinator();
        populate(&coordinator, count);
        group.bench_function(format!("candidates_{count}"), |b| {
            b.iter(|| {
                let _ = coordinator.candidates(100, "bench").unwrap();
            });
        });
    }

    // Lifecycle transition throughput at scale (all transitions validated).
    let coordinator = open_coordinator();
    populate(&coordinator, 10_000);
    group.bench_function("candidate_then_reclaim_10k", |b| {
        b.iter(|| {
            let candidates = coordinator.candidates(10, "bench").unwrap();
            for cand in candidates {
                let _ = coordinator.reclaim(&reclaim_fabric::protocol::ReclaimRequest {
                    object_id: cand.decision.object_id,
                    actor: "bench".into(),
                    force: false,
                });
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_candidate_selection);
criterion_main!(benches);
