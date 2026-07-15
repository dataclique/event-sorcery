{
  git-hooks,
  pkgs,
  rustToolchain,
  self,
  system,
}:
let
  rustfmtHook = pkgs.writeShellApplication {
    name = "event-sorcery-rustfmt-hook";
    runtimeInputs = [ rustToolchain ];
    text = ''
      cargo fmt --all -- "$@"
    '';
  };
in
git-hooks.lib.${system}.run {
  src = self;
  package = pkgs.prek;

  hooks = {
    cabal-fmt.enable = true;
    fourmolu.enable = true;
    hlint.enable = true;
    nixfmt.enable = true;
    rustfmt = {
      enable = true;
      entry = "${rustfmtHook}/bin/event-sorcery-rustfmt-hook";
    };
    taplo.enable = true;
    trim-trailing-whitespace.enable = true;
  };
}
