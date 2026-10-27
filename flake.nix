{
  description = "event-sorcery: one Rust engine with Rust and Haskell bindings";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
    but-nix.url = "github:dataclique/but.nix";

    git-hooks.url = "github:cachix/git-hooks.nix";
    git-hooks.inputs.nixpkgs.follows = "nixpkgs";

    devenv.url = "github:cachix/devenv";
    devenv.inputs = {
      nixpkgs.follows = "nixpkgs";
      git-hooks.follows = "git-hooks";
    };

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      but-nix,
      flake-utils,
      git-hooks,
      devenv,
      rust-overlay,
      crane,
      ...
    }@inputs:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
        # Preserve the compiler version while replacing the build integration.
        rustToolchain = pkgs.rust-bin.stable."1.94.0".default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        rustPkgs = pkgs.callPackage ./rust.nix { inherit craneLib; };
        rustBuildInputs = [
          rustToolchain
          pkgs.stdenv.cc
          pkgs.pkg-config
          pkgs.openssl.dev
          pkgs.sqlite.dev
        ];
        checkExamples = pkgs.writeShellApplication {
          name = "check-examples";
          meta = {
            description = "Validate the event-sorcery example crates";
            license = pkgs.lib.licenses.mit;
            mainProgram = "check-examples";
          };
          runtimeInputs = rustBuildInputs ++ [
            pkgs.cargo-nextest
            pkgs.nushell
          ];
          text = ''
            export CARGO_INCREMENTAL=0
            export SQLX_OFFLINE=true
            export PKG_CONFIG_PATH="${
              pkgs.lib.makeSearchPath "lib/pkgconfig" [
                pkgs.openssl.dev
                pkgs.sqlite.dev
              ]
            }"
            exec nu ${./scripts/check-examples.nu}
          '';
        };
        but = but-nix.packages.${system}.default;
        haskellPackages = pkgs.haskell.packages.ghc914.override {
          overrides = _final: previous: {
            binary-orphans = pkgs.haskell.lib.doJailbreak previous.binary-orphans;
            microstache = pkgs.haskell.lib.doJailbreak previous.microstache;
          };
        };
        ffiEngine = rustPkgs.engine;
        haskellBinding = pkgs.haskell.lib.overrideSrc (haskellPackages.callPackage
          ./nix/event-sorcery-haskell.nix
          {
            event_sorcery_ffi = ffiEngine;
            linear-base = pkgs.haskell.lib.dontCheck haskellPackages.linear-base;
          }
        ) { src = ./bindings/haskell; };
        haskellBenchmarks = (pkgs.haskell.lib.doBenchmark haskellBinding).overrideAttrs (old: {
          postInstall = (old.postInstall or "") + ''
            mkdir -p $out/bin
            cp dist/build/event-sorcery-benchmarks/event-sorcery-benchmarks $out/bin/
          '';
        });
        hooks = import ./git-hooks.nix { inherit pkgs rustToolchain; };
        hookCheck = git-hooks.lib.${system}.run {
          src = self;
          package = pkgs.prek;
          inherit hooks;
        };
      in
      {
        packages = {
          check-examples = checkExamples;
          inherit but;
          engine = ffiEngine;
          haskell = haskellBinding;
          haskell-benchmarks = haskellBenchmarks;
          rust = rustPkgs.package;
          rust-test = rustPkgs.test;
          rust-clippy = rustPkgs.clippy;
          rust-fmt = rustPkgs.fmt;
        };

        apps.check-examples = {
          type = "app";
          program = pkgs.lib.getExe checkExamples;
          meta = checkExamples.meta;
        };
        apps.haskell-benchmarks = {
          type = "app";
          program = "${haskellBenchmarks}/bin/event-sorcery-benchmarks";
          meta = haskellBenchmarks.meta;
        };

        checks = {
          formatting = hookCheck;
          haskell = haskellBinding;
          rust = rustPkgs.package;
          rust-test = rustPkgs.test;
          rust-clippy = rustPkgs.clippy;
          rust-fmt = rustPkgs.fmt;
          package-version =
            let
              expected = (fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
              versions = map (package: package.version) [
                rustPkgs.package
                rustPkgs.engine
                rustPkgs.test
                rustPkgs.clippy
                rustPkgs.fmt
              ];
            in
            assert pkgs.lib.assertMsg (builtins.all (
              version: version == expected
            ) versions) "Rust package versions must match the workspace manifest.";
            pkgs.writeText "event-sorcery-package-version" expected;
        };

        devShells.default = devenv.lib.mkShell {
          inherit inputs pkgs;
          modules = [
            (
              { config, ... }:
              {
                devenv.dotfile = "${config.devenv.root}/.tmp/devenv";
                packages = [
                  but
                  pkgs.git
                  pkgs.pkg-config
                  pkgs.openssl.dev
                  pkgs.sqlite.dev
                  pkgs.sqlite
                  haskellPackages.ghc
                  pkgs.cabal-install
                  pkgs.cargo-expand
                  pkgs.cargo-nextest
                  pkgs.rust-cbindgen
                  pkgs.fourmolu
                  pkgs.haskellPackages.cabal-fmt
                  pkgs.hlint
                  pkgs.nixfmt
                  pkgs.nushell
                  pkgs.prek
                  pkgs.sqlx-cli
                  pkgs.stack
                ];
                languages = {
                  nix.enable = true;
                  rust = {
                    enable = true;
                    toolchain.rustc = rustToolchain;
                    toolchain.cargo = rustToolchain;
                    toolchain.rustfmt = rustToolchain;
                    toolchain.clippy = rustToolchain;
                  };
                };
                env = {
                  SQLX_OFFLINE = "true";
                  CARGO_INCREMENTAL = "0";
                };
                git-hooks = {
                  inherit hooks;
                  package = pkgs.prek;
                };
                tasks."event-sorcery:gitbutler-hooks" = {
                  after = [ "devenv:git-hooks:install" ];
                  before = [ "devenv:enterShell" ];
                  exec = ''
                    hooks_dir="$(git rev-parse --git-path hooks)"
                    if grep -q 'generated by prek' "$hooks_dir"/pre-commit 2>/dev/null; then
                      if grep -q GITBUTLER_MANAGED_HOOK_V1 "$hooks_dir"/pre-commit.legacy 2>/dev/null; then
                        mv "$hooks_dir"/pre-commit "$hooks_dir"/pre-commit-user
                        mv "$hooks_dir"/pre-commit.legacy "$hooks_dir"/pre-commit
                      fi
                    fi
                  '';
                };
              }
            )
          ];
        };

        formatter = pkgs.nixfmt-tree;
      }
    );
}
