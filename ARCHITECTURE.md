# Reclaim Fabric Architecture

**Reclaim Fabric** is a vendor-neutral machine-state reclamation runtime for AI
infrastructure. This document describes the complete system: the object model, the
lifecycle state machine, reclamation economics, lineage safety, deduplication,
compression, archival, persistence, recovery, transport, authority, and the
coordinator/node runtime.

## 1. Positioning

Reclaim Fabric is the fourth runtime in an accelerator-infrastructure sequence:

- **FlashTier** — where do the bytes live? (physical byte residency)
- **Context Fabric** — where does accumulated reusable computation live? (residency of
  reusable computational state)
- **Compute Fabric** — where should the next computation run? (execution placement)
- **Reclaim Fabric** — **what state is still worth keeping?** (state lifecycle and reclamation)

Reclaim Fabric owns exactly one authority domain: **reclamation authority** (deciding what
may be released, when, and why). It distinguishes:

1. **physical placement** — tracked through backend descriptors; owned by storage layers
   (FlashTier-like), never by Reclaim Fabric;
2. **logical state identity** — the `ReclaimObject` model;
3. **execution placement** — tracked through recomputation recipes; owned by schedulers
   (Compute Fabric-like);
4. **reclamation authority** — owned by Reclaim Fabric.

The runtime is workload-independent: no transformers, KV-cache assumptions, PyTorch,
vLLM, Kubernetes, or Ray concepts exist in the core.

## 2. Design principles

1. **Vendor-neutral** — no hard dependency on NVIDIA/CUDA, one cloud, one model server,
   one storage vendor, one orchestrator, or one model architecture. The `Backend` trait is
   the extension point for accelerator integrations; the shipped backends are CPU host
   memory and local filesystem, and this release has no CUDA/GPU Cargo feature.
2. **Workload-independent** — the core understands *state*, not models.
3. **Deterministic authority** — identical authoritative inputs + policy version
   ⇒ identical decisions (verified by property tests).
4. **Explicit object identity** — 128-bit UUIDs.
5. **Explicit generations** — mutation produces generation/version semantics.
6. **Explicit lineage** — typed DAG edges (`DERIVES_FROM`, `DEPENDS_ON`, `SUPERSEDES`,
   `DUPLICATES`).
7. **Explicit economics** — named cost components, versioned weights.
8. **Fail-safe reclamation** — never destroy state when authority, dependency safety, or
   survivability cannot be proven.
9. **Durable decisions** — decisions, transitions, and audit survive restart.
10. **Replayable reasoning** — every decision carries named components and reasons.
11. **Bounded control-plane use** — bounded frames, connection counts, query counts, and CLI reads.
12. **Fenced shutdown and restart** — built-in workers are drained within explicit deadlines.

## 3. Object model

`ReclaimObject` (src/object.rs) is the first-class tracked unit:

| Field | Meaning |
|---|---|
| `id` (128-bit UUID) | stable global identity |
| `generation` | mutation generation |
| `class` | opaque application class ("checkpoint", "kv-cache", …) |
| `logical_size`, `physical_size`, `compressed_size` | size accounting |
| `created_at_ms`, `last_access_ms`, `access_count` | access tracking |
| `reuse_probability`, `reuse_horizon_secs` | expected reuse profile |
| `recompute_cost`, `recompute_latency_secs` | reconstruction economics |
| `transfer_cost`, `migration_cost` | movement economics |
| `storage_cost_per_byte_sec`, `memory_cost_per_byte_sec` | retention cost rates |
| `replication_count`, `durability_class` | physical redundancy contract |
| `survivability_class` | policy-selector vocabulary for workload survivability |
| `owner` | authority identity |
| `content_hash` | SHA-256 content identity |
| `lifecycle_state` | current state machine state |
| `policy_version`, `decision_epoch` | decision provenance |
| `pinned`, `protected` | hard invariants |
| `min_retention_deadline_ms` | enforced earliest automatic-reclamation time |
| `max_retention_deadline_ms` | persisted desired upper bound; not forced by the built-in policy |
| `app_metadata` | typed JSON application metadata (the only free-form field) |

`Replica` records physical placement: `backend` id, `key`, `kind`
(`HOT`/`DURABLE`/`ARCHIVED`), size, content hash, verification time, validity, and the
owning node.

### Durability classes

| Class | Minimum valid copies | Semantics |
|---|---|---|
| `EPHEMERAL` | 0 | may be reclaimed aggressively |
| `RECOMPUTABLE` | 0 | may be reclaimed if reconstruction holds |
| `DURABLE` | 1 | must retain at least one valid durable copy |
| `CRITICAL` | 2 | must retain configured redundancy |

When registration includes payload bytes, a `DURABLE`/`CRITICAL` object with fewer than
the minimum copies is rejected. Reclamation that would drop a live object below its
durability minimum is rejected with `SurvivabilityViolation`. `SurvivabilityClass` is a
separate policy matching vocabulary and does not define these copy counts.

## 4. Lifecycle state machine

Strict, static transition table (src/lifecycle.rs). Illegal transitions fail
deterministically with `InvalidTransition`; repeated requests are idempotent no-ops;
every committed transition is persisted and audited.

The diagram is transition vocabulary, not an automatic tiering pipeline. Built-in
operations do not automatically demote objects to `COLD` or mark them `DEDUPED`;
content-deduplication ownership is tracked independently of lifecycle state.

```text
CREATED ──► HOT ──► WARM ──► COLD ──► COMPRESSED
   │        ││        │         ││        │
   │        │└────────┴─────────┘└───► DEDUPED
   └──► WARM │         │               ││
             │         └──► ARCHIVED ──┘│
             │              │           │
             ▼              ▼           ▼
          RECOMPUTABLE ◄────┘      RECLAIM_PENDING ──► RECLAIMED
             │  ▲                        │
             └──┘                        └──► (revival) HOT/WARM
```

`FAILED` is terminal (operator repair; a failed physical reclaim never commits
`RECLAIMED`). `RECLAIMED` is terminal.

## 5. Reclamation economics

Decisions are computed over **named components** (src/economics.rs), each recorded in the
explanation:

```text
ExpectedKeepValue =
    ExpectedReuseValue
  + ReconstructionAvoidanceValue
  + DependencyValue
  + SurvivabilityValue

ExpectedKeepCost =
    MemoryCost + StorageCost + ReplicationCost + PressureSurcharge
  + TransferCost + MigrationCost

ReclaimScore = ExpectedKeepCost − ExpectedKeepValue
```

- Inputs are *accepted* from workloads or *derived* from metadata (`CostInputs::derive`).
- `PressureSurcharge` is zero at normal pressure and adds only the incremental
  pressure multiplier above the baseline memory cost.
- Weights are policy-owned and versioned (`CostWeights`).
- The verdict additionally requires `reuse_probability ≤ policy.min_reuse_probability`.
- Every decision persists: score, threshold, policy id+version, epoch, all components,
  and human-readable reasons. Deterministic replay is tested.

## 6. Policy engine

Policies are serializable, versioned documents (src/policy.rs) matched by specificity:

```text
emergency (CRITICAL) > pressure-level > object-class > owner > durability > survivability > default
```

- Emergency policies may run at CRITICAL pressure with looser thresholds but can never
  override pins, protection, or survivability.
- Policy changes never mutate audit history; every decision records the exact
  `id-version` used.
- Policies persist in the store and reload at restart.

## 7. Lineage and dependency safety

The lineage graph (src/lineage.rs) is a typed DAG with cycle rejection at insertion and
full validation (Kahn topological sort) after restart. Reclamation safety rule:

> A live, non-reconstructible object blocks reclamation of any ancestor reachable
> through a path consisting entirely of `DEPENDS_ON` edges.

`DERIVES_FROM`, `SUPERSEDES`, and `DUPLICATES` edges do not propagate reclamation
hazards. Supersession enables the checkpoint pattern:
`generation_11 SUPERSEDES generation_10` ⇒ generation 10 is reclaimable.

## 8. Deduplication

- Content identity = SHA-256 (`ContentHash`).
- `dedup` table: `content_hash → (backend, key, ref_count, payload_size)`.
- Registration of identical payloads increments ref counts; replicas reference the
  canonical key.
- Reclamation releases a reference; the physical payload is deleted **only when the
  last live reference is released**. This is an invariant (property-tested).
- CRC-32C is used for fast framed-transport corruption checks; archives use
  SHA-256 content identity and verification.

## 9. Compression and archival

- `CompressionCodec` trait; built-in `ZstdCodec` (BSD-3-Clause) and `NoopCodec`.
- Compression is integrity-verified before and after (a size-bounded round-trip must
  reproduce the original).
- `ArchiveBackend` trait; built-in `LocalFsArchive` writes a uniquely named temporary
  file, fsyncs it, atomically publishes it without replacement by hard-linking it to a
  SHA-256-derived flat filename, and verifies it again. Logical keys cannot alias through
  separator replacement and traversal keys are rejected. A read/delete compatibility path
  preserves archives created with the earlier separator-flattened layout; new writes never
  use that ambiguous layout.
- Archive is only recorded after integrity verification; a corrupt archive can never
  justify deleting the last valid source copy.
- `FileBackend` publishes payloads through a uniquely named same-directory temporary file.
  Normal failures remove their owned temporary file. A process kill can leave `.tmp-*`
  files; they are excluded from payload listing and byte accounting but are not
  automatically deleted because a backend root may be shared with another live process.

## 10. Persistence

SQLite (bundled) via `Store` (src/persistence.rs), schema versioned by
`PRAGMA user_version`. WAL mode, `synchronous=FULL`, a bounded `busy_timeout`, open-time
required-table/column probes, and SQLite `quick_check`. Unversioned nonempty databases and
versioned partial schemas are rejected rather than silently initialized. Tables:

`objects, replicas, lineage, dedup, decisions, attempts, reservations, coordinator,
archives, failures, audit, journal, policies`.

- Opening a store with an unsupported schema version fails loudly; incompatible state is
  never silently discarded.
- Unsigned metadata is range-checked at the SQLite boundary; malformed negative values,
  non-canonical hashes/UUIDs, invalid booleans, and corrupt persisted JSON fail closed.
- Audit/failure replay limits are capped and rejected when pathological rather than being
  converted into SQLite's negative (unbounded) `LIMIT` behavior.
- Audit is append-only (verified by a dedicated test).
- `Store` is cloneable (Arc-shared connection) and never held across blocking I/O.

## 11. Transactional reclaim and recovery

The reclaim lifecycle (src/coordinator.rs):

```text
1. candidate selected (policy decision, persisted)
2. decision produced (score, threshold, policy, explanation)
3. authority reserved (attempt + reservation + journal RESERVED in one SQLite transaction)
4. dependency graph revalidated (after reservation, before execution)
5. survivability revalidated (min valid copies)
6. physical reclamation attempted (dedup-aware, journal PHYSICAL_STARTED/DONE)
7. physical result verified (payload exists ⇒ fail closed)
8. metadata committed (replicas, archives, dedup refs, state, journal COMMITTED)
9. object transitions to RECLAIMED
10. audit record persisted
```

A failure before physical execution is rolled back. Once physical execution has started,
an unclassifiable backend error preserves the open journal and `RECLAIM_PENDING` state,
records the failure, and fences/shuts down that coordinator so restart recovery can inspect
physical truth. It is never guessed into `RECLAIMED` or terminal `FAILED` state.

Crash reconciliation (src/recovery.rs) at restart:

| Journal phase at crash | Physical truth | Recovery action |
|---|---|---|
| `RESERVED`/`VALIDATED` | any | roll back to prior state |
| `PHYSICAL_STARTED` | every explicitly planned last-owner payload gone | commit (reality wins) |
| `PHYSICAL_STARTED` | every explicitly planned payload present | roll back |
| `PHYSICAL_STARTED` | empty physical plan | roll back any partial metadata-only release |
| `PHYSICAL_DONE` | every explicitly planned payload gone | commit |
| `PHYSICAL_DONE` | every explicitly planned payload present | roll back |
| `PHYSICAL_DONE` | empty physical plan | commit the completed shared-reference-only release |
| physical descriptors disagree (some present, some gone) or cannot be queried | mixed/unknown | leave journal open and report a recovery error |
| none (expired reservation) | — | mark `FAILED` (fail closed) |

Dedup ref counts and canonical keys are recomputed from live replicas. Missing rows are
recreated, stale rows are removed, and drift is repaired; inconsistent replica metadata
is reported rather than guessed. Lineage is validated after restart; corrupt lineage is
reported, never guessed.

## 12. Transport

Framed TCP (src/transport.rs):

```text
[magic u32=0x52463100][version u16=1][type u8][flags u8][len u32][crc32c u32][payload]
```

- Peer-provided lengths are never trusted beyond `MAX_FRAME_SIZE` (16 MiB).
- Malformed frames reject the connection (fail closed); checksums are mandatory.
- Requests carry ids; replies echo them; timeouts bound reads and writes.
- The server bounds connection concurrency and drains gracefully on shutdown.
- Windows note: accepted sockets are explicitly restored to blocking mode (they inherit
  the listener's non-blocking mode).

## 13. Authority and coordination

- The coordinator claims the store with a process id + boot id; claims bump an epoch;
  live foreign claims are rejected; clean shutdowns release the claim so restarts are
  immediate; crashes rely on the stale window.
- Nodes register `name@pid@boot-id` with their own listen address and namespaced backend
  ids; heartbeats refresh; timed-out nodes are retired.
- Node operation handlers reject any request whose `coordinator_epoch` does not match the
  last known epoch (stale-authority rejection, tested end to end).
- Two workers can never destroy the same state: open reservations serialize reclaims,
  and reservations/journal make recovery authoritative.

## 14. Concurrency

- The coordinator serializes authoritative mutations through one mutex-protected inner
  state; physical operations happen **outside** the lock between journal phases.
- SQLite writes are serialized; reads are lock-scoped and never nested (a reentrant-lock
  deadlock found during hardening was fixed with lock-scoped collection).
- No unbounded task spawning: the server caps connections; heartbeats are single-threaded
  loops.

## 15. Error model

Typed errors (src/errors.rs) with stable machine-readable classes that travel intact
over the wire: `InvalidTransition`, `StaleEpoch`, `StaleAttempt`, `ReservationConflict`,
`DependencyViolation`, `SurvivabilityViolation`, `PinnedObject`, `ProtectedObject`,
`IntegrityFailure`, `ArchiveFailure`, `CompressionFailure`, `Transport`, `Protocol`,
`Persistence`, `Recovery`, `Policy`, `Backend`, `Pressure`, `NotFound`,
`GenerationMismatch`, `InvalidArgument`, `Io`, `Dedup`, `Internal`. No panics on normal
runtime failures.

## 16. Security model

See SECURITY.md. In brief: all network input is validated (lengths, enums, ids,
generations, policy ids); archive/backend keys reject path traversal; no deserialization
of executable types; `unsafe` is not used in the core.

## 17. Directory layout

```text
src/
  archive.rs     archival abstraction + durable local filesystem backend
  audit.rs       append-only audit replay/inspection tooling
  backends.rs    payload backends (memory, filesystem) + registry
  cli.rs         CLI + coordinator transport handler
  compression.rs pluggable codecs + verified round trips
  coordinator.rs authority, reclaim transaction, node registry
  dedup.rs       content-identity reference ownership
  economics.rs   named cost components and decision surface
  errors.rs      typed error model
  integrity.rs   SHA-256 + CRC-32C
  lifecycle.rs   strict transition table
  lineage.rs     typed DAG + dependency safety
  node.rs        node runtime
  object.rs      object model
  persistence.rs SQLite store
  policy.rs      versioned policies + resolution
  pressure.rs    pluggable pressure sources + levels
  protocol.rs    wire method names + payload types
  recovery.rs    journal reconciliation + dedup repair
  transport.rs   framed TCP server/client
tests/           integration, property invariants, multi-process
benches/         criterion benchmarks (1K/10K/100K objects)
examples/        12 runnable examples
```

## 18. Limitations (1.0.0)

- Control-plane payload transfer is bounded by the 16 MiB frame; large payloads should be
  registered by nodes through backend descriptors.
- One coordinator per store file (single-writer authority); multi-coordinator HA is out of
  scope.
- Node ↔ coordinator connections are client-initiated; nodes must be reachable at their
  registered addresses.
- The node registry is in-memory; nodes re-register after coordinator restart.
- GPU/accelerator backends are designed-for but not shipped (CPU-only core).
- `LocalFsArchive` requires a local filesystem that supports hard links. File contents are
  fsynced on every supported platform; parent-directory fsync is additionally performed on
  Unix, while Windows directory-entry power-loss guarantees remain filesystem-dependent.
- The audit table is append-only, and object state updates use atomic object+audit helpers.
  Lineage, policy, and in-memory node-registry mutations are not a complete transactional
  event log; a process crash between those mutations and their audit append can omit an
  event.
- Compression and archive workflows coordinate physical I/O with metadata compensation,
  but physical backend I/O cannot participate in the SQLite transaction. Sudden process or
  power loss at those boundaries can require operator cleanup of an orphan payload/record.
- Object creation likewise has a crash window between staged replica/dedup metadata and final
  object publication. In-process failures compensate; sudden termination can leave an orphan
  that prevents automatic recovery until repaired.
- A coordinator restart cannot resolve an open reclaim journal for a node-hosted backend before
  that node can re-register, because node routes are not persisted. Remote journal recovery
  therefore requires operator intervention in this release.
- A crash or failure between multiple planned physical deletions can leave mixed presence.
  Recovery keeps the journal open rather than guessing or automatically resuming the plan.
- Local in-memory replicas are not automatically invalidated merely because the coordinator
  process restarted; use durable backends where restart survivability matters.
- Socket timeouts are per I/O call, not absolute per-frame deadlines. A slow-drip peer can retain
  a bounded connection slot. The control plane must remain on a protected network.
- Custom `Backend`, `ArchiveBackend`, `PressureProvider`, and request-handler implementations are
  trusted to return. Rust cannot forcibly cancel a callback blocked in foreign or OS code.
- Dataset-wide planning/recovery and persisted policy/object state scale with the dataset; the
  frame/query caps are not a global memory or storage quota.
- No dashboard/frontend; CLI + JSON only.
