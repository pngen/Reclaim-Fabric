# Reclaim Fabric

**Reclaim Fabric is a vendor-neutral machine-state reclamation runtime for AI infrastructure.**

It answers the question:

> **What state is still worth keeping?**

Reclaim Fabric produces **retain-or-reclaim** policy decisions from expected reuse value,
reconstruction cost, lineage, dependency structure, resource pressure, migration cost,
persistence cost, and durability requirements. Compression, deduplication, archival, and
restoration are explicit lifecycle operations exposed by the runtime.

It is a distinct systems runtime with its own authority, state machine, economics,
persistence, recovery, transport, CLI, examples, tests, benchmarks, and documentation.

## The stack

| Runtime | Question it answers | Responsibility |
|---|---|---|
| **FlashTier** | Where do the bytes live? | physical byte residency |
| **Context Fabric** | Where does accumulated reusable computation live? | reusable computational-state residency |
| **Compute Fabric** | Where should the next computation run? | execution placement |
| **Reclaim Fabric** | **What state is still worth keeping?** | **state lifecycle and reclamation** |

Reclaim Fabric integrates conceptually with FlashTier, Context Fabric, and Compute Fabric
through clean interfaces and abstractions, and functions completely independently.

## What Reclaim Fabric is not

- **Not a cache library** — it does not serve lookups or manage hit rates.
- **Not garbage collection** — liveness is decided by economics, lineage, and policy, not by reachability.
- **Not an LRU implementation** — access recency is tracked and auditable, but the built-in
  v1 policy does not currently include it in the score.
- **Not a storage-tiering wrapper** — backend placement is explicit; external storage
  layers remain responsible for physical tiering.
- **Not a model-serving framework** and **not a model-specific KV cache** — the core is workload-independent.

## Core capabilities

1. Register reusable computational-state objects with explicit identity, generations, and lineage.
2. Track a strict, persisted, auditable lifecycle state vocabulary and transition graph
   (`CREATED`, `HOT`, `WARM`, `COLD`, `COMPRESSED`, `DEDUPED`, `ARCHIVED`,
   `RECOMPUTABLE`, `RECLAIM_PENDING`, `RECLAIMED`, and `FAILED`). This is not an
   automatic linear pipeline: built-in operations do not automatically emit `COLD` or
   `DEDUPED`, and deduplication reference ownership is orthogonal to lifecycle state.
3. Compute retention economics from named cost components, versioned weights, and documented
   baseline estimate constants.
4. Produce deterministic, replayable decisions under versioned, serializable policies.
5. Reclaim state through a journaled protocol: `plan → reserve → validate → execute → verify → commit`.
6. Respect hard invariants: pins, protection, durability minimums, dependency safety,
   and deduplication reference ownership.
7. Compress (pluggable codecs, Zstandard built in) and archive (durable local filesystem,
   atomic writes, integrity verified) state.
8. Deduplicate by SHA-256 content identity; shared payloads survive until the last reference.
9. Coordinate multiple processes over a framed TCP control plane with coordinator epochs,
   stale-writer rejection, and node heartbeats.
10. Recover after crashes: the recovery journal reconciles physical truth with metadata truth
    at restart.
11. Bound control-plane frames, query counts, connection concurrency, and CLI file reads.
    Dataset-sized planning/recovery operations still scale with persisted state.

## Quick start

```text
# Build
cargo build --release

# Run a coordinator (foreground)
cargo run --release -- coordinator start --store fabric.db --data-dir data --archive-dir archive

# Create an object with a payload and explicit economics
cargo run --release -- object create \
  --class checkpoint --data-file state.bin --backend memory \
  --reuse-probability 0.01 --recompute-cost 1 --memory-cost-per-byte-sec 1

# Ask the runtime what is still worth keeping
cargo run --release -- plan --candidates
cargo run --release -- reclaim <OBJECT_ID>
cargo run --release -- audit
```

Client control commands support `--json` for machine-readable output. Foreground
`coordinator start` and `node start` emit a `READY <address>` readiness line.

## Examples

Twelve runnable examples (`examples/`) demonstrate the system end to end:

1. `basic_lifecycle` — register, touch, plan, reclaim
2. `pressure_reclamation` — synthetic pressure drives candidate selection
3. `expensive_recompute_retained` — high reconstruction cost retains
4. `cheap_recompute_reclaimed` — cheap reconstruction reaps
5. `pinned_object` — pinning is a hard invariant
6. `dependency_safe` — lineage-aware reclamation safety
7. `superseded_checkpoint` — generation supersession
8. `dedup_payload` — shared physical payloads
9. `compression_before_archive` — compress, verify, archive, restore
10. `multiprocess` — coordinator + node over framed TCP
11. `crash_restart` — recovery after a crash mid-reclaim
12. `policy_driven` — class- and owner-specific policies

```text
cargo run --example basic_lifecycle
```

## Library API

Reclaim Fabric is usable from another project without shelling out:

```rust
use reclaim_fabric::backends::{BackendRegistry, MemoryBackend};
use reclaim_fabric::coordinator::{Coordinator, CoordinatorConfig, SystemClock};
use reclaim_fabric::pressure::PressureRegistry;
use reclaim_fabric::protocol::CreateObjectRequest;
use reclaim_fabric::object::ReclaimObject;

let mut backends = BackendRegistry::new();
backends.register(std::sync::Arc::new(MemoryBackend::new("memory")))?;
let mut config = CoordinatorConfig::default();
config.store_path = ":memory:".into();
let coordinator = Coordinator::open(config, backends, PressureRegistry::new(), vec![],
    std::sync::Arc::new(SystemClock))?;

let object = ReclaimObject::new(uuid::Uuid::new_v4(), 0, "checkpoint", 4096, 0);
let created = coordinator.create_object(&CreateObjectRequest {
    object,
    payload_b64: Some(reclaim_fabric::base64_payload(b"state")),
    target_backend: Some("memory".into()),
    replicate_to: vec![],
})?;
```

See `src/lib.rs` and the module documentation for the full API.

## CLI reference

```text
reclaim-fabric coordinator start --store PATH --bind ADDR [--data-dir DIR] [--archive-dir DIR]
reclaim-fabric node start --coordinator ADDR --name NAME --bind ADDR --data-dir DIR
reclaim-fabric object create|inspect|touch|pin|unpin|lineage|dependencies
reclaim-fabric lineage add|remove --parent ID --child ID --kind KIND
reclaim-fabric plan --object ID | plan --candidates [--limit N]
reclaim-fabric reclaim ID [--force]
reclaim-fabric compress|archive|restore|verify ID
reclaim-fabric pressure get | pressure set --level LEVEL
reclaim-fabric policy list | policy inspect --id ID --version V | policy add --file policy.json
reclaim-fabric audit [--object ID] [--action ACTION] [--limit N]
reclaim-fabric failures [--limit N]
reclaim-fabric stats
reclaim-fabric recover
reclaim-fabric shutdown --reason TEXT
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — the full system design.
- [ROADMAP.md](ROADMAP.md) — implemented 1.0.0 scope and explicitly future work.
- [BENCHMARKS.md](BENCHMARKS.md) — benchmark methodology and reporting rules.
- [SECURITY.md](SECURITY.md) — trust boundaries and vulnerability reporting.
- [CONTRIBUTING.md](CONTRIBUTING.md) — how to build, test, and contribute.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — community participation standards.
- [CHANGELOG.md](CHANGELOG.md) — release history.

## Development

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --release --all-features
cargo bench
```

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Dependency licenses: SQLite (public domain, bundled via rusqlite), Zstandard (BSD-3-Clause),
uuid/serde/serde_json/thiserror/clap/log/env_logger/base64/ctrlc (MIT/Apache-2.0),
sha2 (MIT/Apache-2.0), and crc32c (BSD-2-Clause).
