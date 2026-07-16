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
        haskellBinding = haskellPackages.callCabal2nix "event-sorcery" ./bindings/haskell { };
        rustBuildInputs = rainix.rust-build-inputs.${system};
        rustToolchain = rainix.rust-toolchain.${system};
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
          haskell = haskellBinding;
        };

        checks = {
          formatting = hooks;
          haskell = haskellBinding;
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
