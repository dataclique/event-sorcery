//! Generates the C header from the exported ABI into Cargo's build output.
//!
//! The header is a derived build artifact and must not be maintained by hand.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::C)
        .with_include_guard("EVENT_SORCERY_H")
        .with_trailer(
            r"
#ifndef EVENT_SORCERY_HASKELL_SHIMS
#define EVENT_SORCERY_HASKELL_SHIMS

static inline int32_t es_hs_open(
    const struct EsBuf *options,
    void **out_store,
    struct EsBuf *out_error
) {
    return es_open(options, (struct EsStore **)out_store, out_error);
}

static inline int32_t es_hs_load_stream(
    void **store,
    const struct EsBuf *request,
    struct EsBuf *out_events,
    struct EsBuf *out_error
) {
    return es_load_stream(
        (struct EsStore **)store,
        request,
        out_events,
        out_error
    );
}

static inline int32_t es_hs_current_version(
    void **store,
    const struct EsBuf *request,
    uint64_t *out_version,
    struct EsBuf *out_error
) {
    return es_current_version(
        (struct EsStore **)store,
        request,
        out_version,
        out_error
    );
}

static inline int32_t es_hs_commit(
    void **store,
    const struct EsBuf *request,
    struct EsBuf *out_error
) {
    return es_commit((struct EsStore **)store, request, out_error);
}

static inline int32_t es_hs_close(void **store) {
    return es_close((struct EsStore **)store);
}

#endif
",
        )
        .generate()?
        .write_to_file(out_dir.join("event_sorcery.h"));
    Ok(())
}
