# Encoding conformance

`encoding-v1.vectors` is the shared deterministic-CBOR byte corpus consumed by
both the Rust engine boundary and the Haskell binding tests. Each line starts
with a stable vector name followed by encoded bytes as decimal octets. Repeating
a name continues that vector on another line.

The textual byte representation keeps boundary changes reviewable while still
requiring both implementations to match the exact same CBOR bytes.
