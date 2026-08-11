# Benchmarks

This document defines the methodology and reporting rules for the Reclaim
Fabric Criterion benchmark suite.

## Methodology

- Benchmarks use fresh in-process coordinators and isolated in-memory or
  temporary-filesystem state.
- Candidate selection is measured at 1,000, 10,000, and 100,000 persisted
  objects.
- Integrity inputs are generated from a fixed seed. UUID generation and host
  scheduling can still affect timing.
- Benchmark values describe the machine and build that produced them; they are
  not embedded as portable performance guarantees.
- A benchmark failure is a failure. Hardware or storage paths are not silently
  replaced by a different backend.

## Suites

| Suite | What it exercises |
| --- | --- |
| `throughput` | Planning, registration, reclaim transactions, dedup lookup, and persisted object reads. |
| `candidate_selection` | Bounded candidate queries at 1K/10K/100K objects and candidate-plus-reclaim behavior. |
| `integrity` | SHA-256, CRC-32C, verified Zstandard compression, archive writes, and archive verification. |

## Running

```sh
cargo bench
cargo bench --bench throughput
cargo bench --bench candidate_selection
cargo bench --bench integrity
```

Use `cargo bench --no-run` when validating compilation without publishing
machine-specific performance numbers.
