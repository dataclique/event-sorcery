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
        but = but-nix.packages.${system}.default;
        haskellPackages = pkgs.haskell.packages.ghc914;
        haskellBindingBase = haskellPackages.callCabal2nix "event-sorcery" ./bindings/haskell {
          event_sorcery_ffi = ffiEngine;
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
        engineSource = pkgs.runCommand "event-sorcery-engine-source" { } ''
          mkdir -p $out
          cp ${./Cargo.toml} $out/Cargo.toml
          cp ${./Cargo.lock} $out/Cargo.lock
          cp -R ${./crates} $out/crates
          cp -R ${./.sqlx} $out/.sqlx
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
          inherit but;
          engine = ffiEngine;
          haskell = haskellBinding;
        };

        checks = {
          formatting = hooks;
          haskell = haskellBinding;
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
          '';
        };

        formatter = pkgs.nixfmt-tree;
      }
    );
}
