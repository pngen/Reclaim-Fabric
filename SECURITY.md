# Security

## Trust boundaries

```
┌──────────────┐        framed TCP        ┌──────────────┐
│ CLI / client │ ◄──────────────────────► │  Coordinator │ ── SQLite store (authority)
└──────────────┘                          └──────┬───────┘
                                                 │ framed TCP (node operations)
                                                 ▼
                                          ┌──────────────┐
                                          │     Node     │ ── backends (payload bytes)
                                          └──────────────┘
```

| Boundary | Trust | Notes |
|---|---|---|
| Client → Coordinator | **untrusted** | every frame is validated; no authentication in 1.0 (bind to loopback or a protected network) |
| Coordinator → Node | **untrusted input** | epochs fence stale coordinators but are not credentials |
| Node → Coordinator | **untrusted input** | process/boot ids distinguish registrations but do not authenticate them |
| Store file | **trusted local** | SQLite WAL; version, required schema shape, and B-tree integrity are checked at open |

## Input validation

- **Frames**: magic, protocol version, message type, length (≤ 16 MiB), CRC-32C.
  Malformed frames fail closed and drop the connection.
- **Lengths**: peer-provided lengths are never trusted beyond `MAX_FRAME_SIZE`; reads are
  bounded (`read_exact` against the cap).
- **Enums**: lifecycle states, pressure levels, edge kinds, message types are validated
  before use; unknown values are rejected.
- **IDs/generations**: UUIDs and generations are parsed strictly.
- **Policy identifiers**: policy documents are validated before registration (finite
  thresholds, in-range reuse bounds, emergency ⇒ CRITICAL).
- **Archive/backend keys**: traversal components, backslashes, absolute paths, control
  characters, Windows alternate-stream/device spellings, and non-portable case/Unicode
  identities are rejected. Archive logical separators are mapped to a flat SHA-256-derived
  filename, so path traversal cannot reach outside the canonicalized archive root.

## Reclamation safety (fail closed)

- Pinned and protected objects can never be reclaimed, including by emergency policies
  and `--force`.
- Survivability minimums (DURABLE ≥ 1 copy, CRITICAL ≥ 2) are invariants.
- Non-reconstructible dependents block reclamation of their dependency ancestors.
- Deduplicated payloads are destroyed only when the last live reference is released.
- A failed physical reclaim never produces a `RECLAIMED` metadata state.
- Recovery never guesses: unverifiable or mixed physical state leaves the journal open and
  reports an error; it is never committed as reclaimed.

## DoS resistance

- Bounded connection concurrency (64 by default).
- Bounded frames (16 MiB) and bounded read windows.
- Bounded runtime compression round-trip decompression and capped audit/failure query limits.
- No unbounded task or thread spawning.
- Heartbeats are single-threaded; stale node registrations are retired.

The TCP protocol is plaintext and unauthenticated. Run it only on loopback or a
separately authenticated, protected network. Any reachable peer can issue control
methods, and an epoch must not be treated as a secret. Frame size and connection
count are bounded, but persisted object/policy growth and dataset-wide operations
are not tenant quotas. Socket timeouts are per I/O call; a deliberately slow-drip
peer is a remaining limitation.

## No unsafe code

The core contains no `unsafe` blocks. Deserialization is limited to `serde_json` of
fixed, self-describing protocol types — never executable types.

## Vulnerability reporting

Please report security issues through GitHub's private vulnerability-reporting channel
for this repository, rather than filing a public issue. Include:

- affected version,
- a minimal reproducer,
- the trust boundary crossed,
- impact and any suggested mitigation.

Maintainers will acknowledge within 48 hours and coordinate a fix and disclosure.
