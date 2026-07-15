{
  git-hooks,
  pkgs,
  rustToolchain,
  self,
  system,
}:
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
      package = rustToolchain;
    };
    taplo.enable = true;
    trim-trailing-whitespace.enable = true;
  };
}
