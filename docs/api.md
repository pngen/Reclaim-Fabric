# API Surface (1.0.0)

The runtime is used through the `reclaim_fabric` library or the `reclaim-fabric` CLI.
Both speak the same semantics; the library never shells out.

## Library modules

| Module | Purpose |
|---|---|
| `reclaim_fabric::coordinator` | `Coordinator` runtime: object registration, planning, journaled reclaim, compress/archive/restore/verify, node registry, recovery. `CoordinatorConfig`, `Clock`/`SystemClock`/`FrozenClock`. |
| `reclaim_fabric::object` | `ReclaimObject`, `Replica`, `PhysicalLocation`, `DurabilityClass`, `SurvivabilityClass`, `PhysicalKind`. |
| `reclaim_fabric::lifecycle` | `LifecycleState`, `check_transition`. |
| `reclaim_fabric::economics` | `CostInputs`, `CostWeights`, `Decision`, `DecisionComponents`, `ReclaimVerdict`, `evaluate`. |
| `reclaim_fabric::policy` | `Policy`, `PolicyRegistry`, `PolicyKind`, `decide`. |
| `reclaim_fabric::lineage` | `LineageGraph`, `EdgeKind`, dependency safety, supersession. |
| `reclaim_fabric::dedup` | content identity reference ownership. |
| `reclaim_fabric::compression` | `CompressionCodec`, `ZstdCodec`, `NoopCodec`. |
| `reclaim_fabric::archive` | `ArchiveBackend`, `LocalFsArchive`, `ArchiveRecord`. |
| `reclaim_fabric::backends` | `Backend`, `MemoryBackend`, `FileBackend`, `BackendRegistry`. |
| `reclaim_fabric::persistence` | `Store` (SQLite), journal/attempt/reservation/audit records. |
| `reclaim_fabric::recovery` | `reconcile_store`, `repair_dedup_counts`, journal payload helpers. |
| `reclaim_fabric::transport` | framed TCP `Server`, `Client`, `Request`, `Reply`. |
| `reclaim_fabric::node` | `Node`, `NodeConfig`. |
| `reclaim_fabric::pressure` | `PressureLevel`, `PressureMetrics`, `PressureRegistry`, `SyntheticPressureProvider`. |
| `reclaim_fabric::protocol` | wire method names and payload types. |
| `reclaim_fabric::integrity` | `ContentHash`, `crc32c`, `verify_sha256`. |
| `reclaim_fabric::audit` | append-only audit replay/inspection. |
| `reclaim_fabric::cli` | `build_handler` (embed the coordinator control plane in your own process). |

## Canonical flows

```text
create_object → plan → candidates → reclaim        (registration → decision → reclamation)
create_object → compress → archive → restore       (footprint reduction)
create_object(+payload) → dedup                      (content identity sharing)
node.start + coordinator.open → multi-process       (framed TCP fabric)
coordinator.recover                                  (crash reconciliation)
```

## Error handling

Every fallible API returns `Result<T, ReclaimError>`; `ReclaimError::class()` gives a
stable machine-readable class. Errors survive process boundaries as `WireError`
(class + message) and are re-typed on the client side.

`CompressionCodec::decompress_bounded` is the appropriate entry point for compressed
bytes from an untrusted or corrupted source. Runtime compression verification uses the
known original length as this bound. Audit and failure replay reject limits above 100,000.

## Determinism contract

Given identical objects, pressure, and policy versions, `plan`/`candidates`/`reclaim`
produce identical decisions (frozen clocks are provided for replay testing).
