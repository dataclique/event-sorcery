use std::path::PathBuf;

fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest directory");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("build output directory"));
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

static inline int32_t es_hs_close(void **store) {
    return es_close((struct EsStore **)store);
}

#endif
",
        )
        .generate()
        .expect("generate event_sorcery.h")
        .write_to_file(out_dir.join("event_sorcery.h"));
}
