# 0.4.0 engine boundary threat model

## Trust boundaries

- Arbitrary caller bytes entering deterministic CBOR decoders and every C ABI
  export.
- Caller-managed input pointers, engine-managed output buffers, opaque handles,
  and close/finalizer races across the C ABI.
- Database rows entering record decoders and kernel load-decide-enact paths.
- Domain payloads crossing between a binding and the engine as opaque bytes.
- Stored event envelopes entering binding-side replay and domain folds.

## Assets

- Immutable event history, contiguous per-stream sequences, and commit order.
- Job `ClaimId` fences, claim sequences, attempt and claim budgets, and retained
  terminal outcomes.
- Snapshots, schema versions, and materialized projection state.
- ABI memory ownership, runtime liveness, and process integrity.
- Auditability without disclosing opaque domain payloads in errors or logs.

## STRIDE analysis and required abuse tests

| Threat                 | Boundary abuse                                                                            | Required control and test                                                                                                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spoofing               | Event or job metadata claims another stream or kind                                       | Validate identity and reject mismatched metadata through real replay and worker paths                                                                                                       |
| Tampering              | Non-canonical CBOR, overflow, duplicate stream entries, or a forged or stale claim handle | Strict decoder, checked arithmetic, commit validation, and rejection against the `ClaimId` and persisted claim-sequence fence, including stale-handle tests at boundary values              |
| Repudiation            | A job transition changes state without an audit event                                     | Transaction tests assert the `job` stream event and rebuildable queue projection update together, while idempotent no-ops append nothing                                                    |
| Information disclosure | Decode/storage errors expose opaque payload bytes                                         | Error and log tests enforce the code-specific redaction contract in `SPEC.md` and inspect no raw payload or ABI bytes                                                                       |
| Denial of service      | Oversized buffers, unbounded pages, panic, double-free, or close race wedges the runtime  | Boundary tests exercise every normative byte, depth, payload, commit, page, list, and detail limit in `SPEC.md` at the limit and one beyond it; close-race tests enforce in-flight draining |
| Elevation of privilege | Foreign code obtains a writer path that bypasses engine decisions                         | The erased facade is the sole writer; native and FFI integration tests assert identical fenced outcomes                                                                                     |

Each abuse case MUST first be added as a compiling test that exercises the real
boundary named above and fails because the required control is absent. The
implementation MUST then make that unchanged test pass. New Rust dependencies
require advisory review and build-script inspection before their stack branch is
published.
