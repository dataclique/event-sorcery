# Roadmap

## One engine, native Rust, idiomatic Haskell

Release 0.4.0 makes one Rust engine the authoritative implementation for every
storage transition while preserving typed, idiomatic Rust and Haskell surfaces.
The engine is extracted from the existing `cqrs-es`, Apalis, `sqlite-es`, SQLx,
and SQLite implementation. It is not rebuilt beside it. The first implementation
deliverable characterizes that behavior so extraction cannot silently change it.

```mermaid
flowchart TD
    contract["0.4.0 extraction contract"]
    characterization["#9 Existing behavior characterization"]
    facade["#12 Shared erased Rust facade"]
    parity["Native Rust facade parity"]
    abi["#10 Versioned C ABI and conformance"]
    haskell["#11 Haskell engine binding"]
    protocols["#13 Cross-binding durable protocols"]

    contract --> characterization
    characterization --> facade
    facade --> parity
    facade --> abi
    abi --> haskell
    parity --> protocols
    haskell --> protocols
```

- [ ] [#9 Engine behavior is duplicated across language implementations](https://github.com/dataclique/event-sorcery/issues/9)
      — characterize the current Rust paths before extracting them
- [ ] [#12 The native Rust API does not share an erased engine facade with foreign bindings](https://github.com/dataclique/event-sorcery/issues/12)
- [ ] [#10 Cross-language engine calls lack a stable ABI and conformance contract](https://github.com/dataclique/event-sorcery/issues/10)
- [ ] [#11 The monorepo has no Haskell binding over the native engine](https://github.com/dataclique/event-sorcery/issues/11)
      — retain the typed domain surface and remove duplicated production
      backends and decisions
- [ ] [#13 Durable protocols lack end-to-end cross-binding conformance coverage](https://github.com/dataclique/event-sorcery/issues/13)

## Not epic

- [ ] [#6 sqlite_event_repository is SQLite-specific code inside the backend-agnostic core](https://github.com/dataclique/event-sorcery/issues/6)
