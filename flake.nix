{
  description = "event-sorcery: one Rust engine with Rust and Haskell bindings";

  inputs = {
    rainix.url = "github:rainprotocol/rainix";
    flake-utils.url = "github:numtide/flake-utils";
    but-nix.url = "github:dataclique/but.nix";

    git-hooks.url = "github:cachix/git-hooks.nix";
    git-hooks.inputs.nixpkgs.follows = "rainix/nixpkgs";
  };

  outputs =
    {
      self,
      but-nix,
      flake-utils,
      git-hooks,
      rainix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = rainix.pkgs.${system};
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
            exec nu ${./scripts/check-examples.nu}
          '';
        };
        but = but-nix.packages.${system}.default;
        haskellPackages = pkgs.haskell.packages.ghc914;
        haskellSource = pkgs.runCommand "event-sorcery-haskell-source" { } ''
          mkdir -p $out/bindings/haskell
          cp ${./event-sorcery.cabal} $out/event-sorcery.cabal
          cp -R ${./bindings/haskell}/. $out/bindings/haskell/
          cp -R ${./conformance} $out/conformance
        '';
        haskellBindingBase = haskellPackages.callCabal2nix "event-sorcery" haskellSource {
          event_sorcery_ffi = ffiEngine;
          linear-base = pkgs.haskell.lib.dontCheck haskellPackages.linear-base;
        };
        haskellBinding = haskellBindingBase.overrideAttrs (old: {
          buildInputs = (old.buildInputs or [ ]) ++ [ ffiEngine ];
          configureFlags = (old.configureFlags or [ ]) ++ [
            "--extra-include-dirs=${ffiEngine}/include"
            "--extra-lib-dirs=${ffiEngine}/lib"
          ];
        });
        rustBuildInputs = rainix.rust-build-inputs.${system};
        rustToolchain = rainix.rust-toolchain.${system};
        rustWorkspace = pkgs.rustPlatform.buildRustPackage {
          pname = "event-sorcery-workspace";
          version = "0.4.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          cargoBuildFlags = [
            "--workspace"
            "--all-targets"
            "--all-features"
          ];
          nativeBuildInputs = rustBuildInputs;

          meta = {
            description = "Event Sorcery Rust workspace";
            license = pkgs.lib.licenses.mit;
            platforms = pkgs.lib.platforms.unix;
          };
        };
        engineSource = pkgs.runCommand "event-sorcery-engine-source" { } ''
          mkdir -p $out
          cp ${./Cargo.toml} $out/Cargo.toml
          cp ${./Cargo.lock} $out/Cargo.lock
          cp -R ${./crates} $out/crates
          cp -R ${./.sqlx} $out/.sqlx
          cp -R ${./conformance} $out/conformance
        '';
        ffiEngine = pkgs.rustPlatform.buildRustPackage {
          pname = "event-sorcery-ffi";
          version = "0.4.0";
          src = engineSource;
          cargoLock.lockFile = ./Cargo.lock;
          cargoBuildFlags = [
            "--package"
            "event-sorcery-ffi"
          ];
          installPhase = ''
            runHook preInstall
            mkdir -p $out/include $out/lib
            cp \
              target/${pkgs.stdenv.hostPlatform.rust.rustcTarget}/release/libevent_sorcery_ffi.a \
              $out/lib/
            cp \
              target/${pkgs.stdenv.hostPlatform.rust.rustcTarget}/release/build/event-sorcery-ffi-*/out/event_sorcery.h \
              $out/include/event_sorcery.h
            runHook postInstall
          '';
        };
        hooks = import ./git-hooks.nix {
          inherit
            git-hooks
            pkgs
            rustToolchain
            self
            system
            ;
        };
      in
      {
        packages = rainix.packages.${system} // {
          check-examples = checkExamples;
          inherit but;
          engine = ffiEngine;
          haskell = haskellBinding;
          rust = rustWorkspace;
        };

        apps.check-examples = {
          type = "app";
          program = pkgs.lib.getExe checkExamples;
        };

        checks = {
          formatting = hooks;
          haskell = haskellBinding;
          rust = rustWorkspace;
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [ ffiEngine ];

          nativeBuildInputs =
            rustBuildInputs
            ++ hooks.enabledPackages
            ++ [
              but
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
              pkgs.sqlite
              pkgs.sqlx-cli
              pkgs.stack
            ];

          shellHook = ''
            ${hooks.shellHook}

            if grep -q 'generated by prek' .git/hooks/pre-commit 2>/dev/null; then
              if grep -q GITBUTLER_MANAGED_HOOK_V1 .git/hooks/pre-commit.legacy 2>/dev/null; then
                mv .git/hooks/pre-commit .git/hooks/pre-commit-user
                mv .git/hooks/pre-commit.legacy .git/hooks/pre-commit
              fi
            fi
          '';
        };

        formatter = pkgs.nixfmt-tree;
      }
    );
}
