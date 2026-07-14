# 0.4.0 engine boundary threat model

## Trust boundaries

- Arbitrary caller bytes entering deterministic CBOR decoders and every C ABI
  export.
- Caller-managed input pointers, engine-managed output buffers, opaque handles,
  and close/finalizer races across the C ABI.
- Database rows entering record decoders and kernel load-decide-enact paths.
- Domain payloads crossing between a binding and the engine as opaque bytes.
- Journal and transport envelopes entering binding-side replay, reactor,
  projection, and ingest folds.

## Assets

- Immutable event history, contiguous per-stream sequences, and commit order.
- Job and outbox fencing tokens, attempt and claim budgets, and retained
  terminal outcomes.
- Delivery receipts, checkpoints, snapshots, schema versions, and projection
  state.
- ABI memory ownership, runtime liveness, and process integrity.
- Auditability without disclosing opaque domain payloads in errors or logs.

## STRIDE analysis and required abuse tests

| Threat                 | Boundary abuse                                                                           | Required control and test                                                                                                 |
| ---------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Spoofing               | Metadata or delivery identity claims another stream or source                            | Recompute/validate identity and reject mismatched metadata through real replay and ingest paths                           |
| Tampering              | Non-canonical CBOR, overflow, duplicate stream entries, or forged lease token            | Strict decoder, checked arithmetic, commit validation, and stale-token tests at boundary values                           |
| Repudiation            | A job transition changes state without an audit event                                    | Transaction tests assert every record change and its `$job` event commit together, while idempotent no-ops append nothing |
| Information disclosure | Decode/storage errors expose opaque payload bytes                                        | Malformed-payload tests assert typed redacted causes and inspect no raw bytes in errors or logs                           |
| Denial of service      | Oversized buffers, unbounded pages, panic, double-free, or close race wedges the runtime | Positive limits, bounded decoding, panic poisoning, idempotent close, buffer ownership tests, and concurrent handle tests |
| Elevation of privilege | Foreign code obtains a writer path that bypasses kernels or receipts                     | The erased facade is the sole writer; native and FFI integration tests assert identical fenced outcomes                   |

The abuse tests must first compile and fail against contract stubs, then pass
after implementation. New Rust dependencies require advisory review and build
script inspection before their stack branch is published.
