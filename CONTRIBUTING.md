# Contributing

Thanks for contributing to Reclaim Fabric.

## Ground rules

- Reclaim Fabric is serious systems infrastructure. Favor correctness, explicit
  state, deterministic behavior, inspectability, and honest evidence over
  cleverness.
- Keep the core workload-independent. Framework- or model-specific adapters
  belong above the core abstractions.
- Do not claim functionality that is not implemented. Unsupported capabilities
  must report unsupported.
- Do not weaken tests to make them pass; fix the implementation.
- Keep every target warning-clean under warnings-as-errors.

## Prerequisites

- Rust 1.75+ (stable), Cargo.
- A C toolchain (SQLite is bundled and compiled by `cc`; MSVC Build Tools on Windows,
  gcc/clang elsewhere).

## Development loop

```text
cargo build
cargo test                  # unit + integration + property + multi-process
cargo bench                 # criterion benchmarks
cargo run --example basic_lifecycle
```

## Validation before submitting

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --release --all-features
cargo bench --no-run
```

The multi-process test suite spawns real coordinator/node processes; ensure no stale
`reclaim-fabric` processes remain after test runs
(`Get-Process reclaim-fabric` / `pgrep reclaim-fabric`).

## Hardening expectations

Reclaim Fabric treats its invariants as product requirements. Changes must preserve:

1. RECLAIMED objects have no live physical ownership.
2. Protected objects never reach RECLAIMED automatically.
3. Pinned objects cannot be reclaimed.
4. DURABLE/CRITICAL objects never fall below their minimum valid-copy count.
5. Shared dedup payloads survive while any live reference remains.
6. Non-reconstructible dependencies are never destroyed while required.
7. Stale epochs cannot mutate authoritative state.
8. Stale attempts cannot commit.
9. Invalid lifecycle transitions never commit.
10. Failed physical reclaims never produce RECLAIMED metadata.
11. Restart preserves committed decisions.
12. Identical inputs + policy versions produce identical decisions.

New features must add regression tests, including failure-injection and crash-point tests
where physical state changes.

## Code style

- Follow existing module layout (responsibility-based, not file-count-based).
- No `unsafe` without a documented invariant.
- No hidden heuristics in decisions: add named components, not magic numbers.
- Typed errors only; no panics on normal runtime failures.
- `cargo fmt` formatting.

## License

By contributing you agree that your contributions are licensed under the Apache License
2.0 (see LICENSE). No contributor license agreement is required. Participation is also
subject to [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
