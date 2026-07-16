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
        .generate()?
        .write_to_file(out_dir.join("event_sorcery.h"));
    Ok(())
}
