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
        haskellBinding = haskellPackages.callCabal2nix "event-sorcery" ./bindings/haskell { };
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
          haskell = haskellBinding;
          rust = rustWorkspace;
        };

        apps.check-examples = {
          type = "app";
          program = pkgs.lib.getExe checkExamples;
          meta = checkExamples.meta;
        };

        checks = {
          formatting = hooks;
          haskell = haskellBinding;
          rust = rustWorkspace;
        };

        devShells.default = pkgs.mkShell {
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
          '';
        };

        formatter = pkgs.nixfmt-tree;
      }
    );
}
