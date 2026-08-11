# Changelog

All notable changes to Reclaim Fabric are documented here.
Format: Keep a Changelog (https://keepachangelog.com/). Versioning: SemVer.

## [Unreleased]

Initial implementation of a vendor-neutral machine-state reclamation runtime for AI infrastructure.

### Added

- **Object model**: 128-bit identity, generations, typed metadata, survivability and
  durability classes, explicit economics, pinning/protection, retention deadlines.
- **Lifecycle state machine**: strict persisted, auditable, idempotent transitions across
  11 states; deterministic rejection of illegal transitions.
- **Economics engine**: named cost components, versioned weights, replayable decisions
  with full explanations.
- **Policy engine**: serializable, versioned policies; specificity-based resolution
  (emergency > pressure > class > owner > durability > survivability > default).
- **Lineage**: typed DAG (DERIVES_FROM / DEPENDS_ON / SUPERSEDES / DUPLICATES), cycle
  rejection, dependency-safe reclamation, orphan detection, restart validation.
- **Deduplication**: SHA-256 content identity, reference ownership, collision-safe
  verification, last-reference release.
- **Compression**: pluggable codecs, Zstandard built in, verified round trips,
  benefit estimation.
- **Archival**: durable local filesystem backend, atomic writes, integrity-verified
  transitions.
- **Persistence**: SQLite store with explicit schema versioning; objects, replicas,
  lineage, dedup, decisions, attempts, reservations, coordinator state, archives,
  failures, append-only audit, and a recovery journal.
- **Recovery**: crash reconciliation of the journal against physical truth; dedup
  ref-count repair; fail-closed handling of unresolvable state.
- **Transport**: framed TCP with magic/version/type/CRC, bounded frames, request ids,
  timeouts, malformed-frame rejection, graceful shutdown, bounded concurrency.
- **Coordinator**: coordinator epochs, stale-writer rejection, transactional reclaim
  (plan → reserve → validate → execute → verify → commit), node registry with
  heartbeats and retirement.
- **Nodes**: process identity `name@pid@boot-id`, namespaced backends, epoch-checked
  execution of coordinator-authorized operations.
- **CLI**: coordinator/node runtime commands plus object, lineage, plan, reclaim,
  compress, archive, restore, verify, pressure, policy, audit, failures, stats,
  recover, and shutdown; `--json` output.
- **Examples**: 12 runnable examples covering the full lifecycle, pressure, economics,
  pins, dependencies, supersession, dedup, compression/archive, multi-process operation,
  crash recovery, and policies.
- **Benchmarks**: decision/registration/reclaim/dedup/persistence throughput,
  candidate selection at 1K/10K/100K objects, integrity primitives.
- **Tests**: unit, integration, property invariants, failure injection, and
  multi-process suites; transport security tests (malformed/oversized/truncated frames,
  concurrent clients).
- **Documentation**: README, ARCHITECTURE, SECURITY, CONTRIBUTING.
- **License**: Apache-2.0.

### Fixed during hardening

- Candidate selection was O(n²) (per-object store re-scans); batched preloads remove
  the per-object store re-scan.
- Non-blocking sockets inherited from the listener caused connection aborts on Windows;
  accepted sockets are now explicitly restored to blocking mode.
- `sync_all` on read-only file handles failed on Windows; backend/archive writes now
  open write-capable handles.
- Reentrant store-lock deadlock in `lineage_graph`; lock-scoped collection applied.
- Deduplicated payloads were destroyed on the first reclaim instead of the last
  reference; release now happens under the coordinator lock before physical deletion.
- Dedup accounting was keyed by content hash only; it is now keyed by
  (content hash, backend), one physical payload per backend per content identity.
- Compression wrote a compressed copy without tracking it, orphaning physical bytes;
  compression now atomically replaces the replica (dedup-aware cleanup).
- Registration rollback (under-copy DURABLE/CRITICAL) could delete a shared dedup
  payload; rollback is now dedup-aware.
- Clean coordinator shutdown left a fresh claim, blocking immediate restarts; clean
  shutdowns now release the claim.
- Coordinator restarts rejected same-process-id claims; restarts now bump the epoch.
- Registration of DURABLE/CRITICAL objects below their minimum copy count is now
  rejected at creation.
- Archive keys containing `/` produced invalid temporary paths; keys are flattened.
- Empty-frame/clean-EOF handling and drain behavior validated under repeated runs.

### Security

- Framing, typed payloads, and archive/backend keys are validated; the core contains
  no `unsafe` blocks. The plaintext control plane is unauthenticated and must be
  restricted to loopback or a protected network.
