use std::path::PathBuf;

fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest directory");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("build output directory"));
    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::C)
        .with_include_guard("EVENT_SORCERY_H")
        .generate()
        .expect("generate event_sorcery.h")
        .write_to_file(out_dir.join("event_sorcery.h"));
}
