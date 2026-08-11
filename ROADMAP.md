# Roadmap

This file separates implemented Reclaim Fabric 1.0.0 behavior from possible
future work. Future items are not current features or release guarantees.

## Implemented in 1.0.0

- Strict persisted object lifecycle, lineage, generations, pins, and protection.
- Versioned deterministic retain-or-reclaim policies and auditable economics.
- Journaled reclamation with reservations, stale-authority fencing, verification,
  and restart recovery.
- Deduplication ownership, verified Zstandard compression, local filesystem
  archival, and restoration.
- Coordinator and node processes over bounded, checksummed framed TCP.
- Host-memory and local-filesystem backends with explicit extension interfaces.
- SQLite persistence, integrity checks, CLI and library APIs, tests, examples,
  and Criterion benchmarks.

## Future work

Under consideration; not implemented in 1.0.0:

- Authenticated and encrypted transport for deployments outside a trusted
  cluster network.
- Additional physical backends, including accelerator and object-store adapters.
- Multi-host deployment validation and additional operating-system CI coverage.
- Operator-directed repair tooling for journals intentionally left unresolved
  after unverifiable mixed physical outcomes.
- Incremental or paginated planning for datasets where whole-dataset scans are
  not appropriate.
- Additional policy components and framework-specific adapters above the
  workload-independent core.
