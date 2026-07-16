#!/usr/bin/env nu
# Format, lint, test, and run every example crate.
#
# Examples are excluded from the workspace so the workspace `cargo` commands
# do not see them. This script applies the same checks that workspace CI
# runs, scoped per example via `--manifest-path`.

["simple" "complex"]
| each { |example|
    let manifest = $"examples/($example)/Cargo.toml"
    print $"==> ($example)"
    ^cargo fmt --manifest-path $manifest -- --check
    ^cargo clippy --locked --manifest-path $manifest --all-targets -- -D warnings
    ^cargo nextest run --locked --manifest-path $manifest
    ^cargo run --locked --manifest-path $manifest
    $example
}
| ignore
