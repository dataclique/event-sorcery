# Encoding conformance

`encoding-v1.vectors` is the shared deterministic-CBOR byte corpus consumed by
both the Rust engine boundary and the Haskell binding. It is the one place
where the bytes of the `v1` wire format are written down, so a boundary change
that only one side makes shows up as a failing test on that side.

## Format

Each line starts with a stable vector name followed by that vector's encoded
bytes as decimal octets, separated by whitespace. Repeating a name continues
the same vector on another line, so a long encoding stays inside a readable
line width:

```
commit 133 1 103 97 99 99 111 117 110 116 99 111 110 101 0
commit 129 131 103 67 114 101 97 116 101 100 99 49 46 48
commit 66 0 1
```

The textual byte representation keeps boundary changes reviewable while still
requiring both implementations to produce and accept the exact same CBOR bytes.

## Append-only

Vectors are append-only. A name that exists keeps its bytes: the `v1` format is
what shipped bindings speak, and rewriting a vector silently redefines the
contract instead of breaking the side that diverged. A new encoding gets a new
name; a new wire format gets a new corpus file next to this one.

## Consumers

- The Rust ABI tests in `crates/event-sorcery-ffi` include this file at compile
  time, relative to that crate's manifest directory.
- The Haskell `WireSpec` module reads it at test time as
  `conformance/encoding-v1.vectors`, relative to the package root that Cabal
  runs test suites from.
