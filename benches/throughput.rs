//! Throughput benchmarks: decisions, registration, lifecycle transitions,
//! dedup lookup, and persistence.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
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

fn register_objects(coordinator: &Coordinator, count: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let req = CreateObjectRequest {
            object: sample_object(Uuid::new_v4()),
            payload_b64: None,
            target_backend: None,
            replicate_to: vec![],
        };
        let o = coordinator.create_object(&req).unwrap();
        ids.push(o.id);
    }
    ids
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    group.bench_function("decision_1k_objects", |b| {
        let coordinator = open_coordinator();
        let ids = register_objects(&coordinator, 1_000);
        b.iter_batched(
            || ids.first().copied().unwrap(),
            |id| {
                let _ = coordinator.plan(&id, "bench").unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("registration_1k_objects", |b| {
        let coordinator = open_coordinator();
        b.iter_batched(
            || sample_object(Uuid::new_v4()),
            |o| {
                let req = CreateObjectRequest {
                    object: o,
                    payload_b64: None,
                    target_backend: None,
                    replicate_to: vec![],
                };
                let _ = coordinator.create_object(&req).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("reclaim_transaction", |b| {
        let coordinator = open_coordinator();
        let ids = register_objects(&coordinator, 1_000);
        b.iter_batched(
            || ids.first().copied().unwrap(),
            |id| {
                let _ = coordinator.reclaim(&reclaim_fabric::protocol::ReclaimRequest {
                    object_id: id,
                    actor: "bench".into(),
                    force: true,
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("dedup_lookup_1k_entries", |b| {
        let coordinator = open_coordinator();
        let payload = vec![42u8; 64];
        for _ in 0..1_000 {
            let req = CreateObjectRequest {
                object: sample_object(Uuid::new_v4()),
                payload_b64: Some(reclaim_fabric::base64_payload(&payload)),
                target_backend: Some("memory".into()),
                replicate_to: vec![],
            };
            let _ = coordinator.create_object(&req).unwrap();
        }
        let hash = reclaim_fabric::integrity::ContentHash::of(&payload);
        b.iter(|| {
            let _ = coordinator
                .store()
                .unwrap()
                .get_dedup(&hash, "memory")
                .unwrap();
        });
    });

    group.bench_function("persistence_object_read", |b| {
        let coordinator = open_coordinator();
        let ids = register_objects(&coordinator, 1_000);
        let store = coordinator.store().unwrap();
        b.iter(|| {
            let _ = store.require_object(&ids[0]).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
