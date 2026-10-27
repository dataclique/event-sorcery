{ pkgs, rustToolchain }:
let
  rustfmtHook = pkgs.writeShellApplication {
    name = "event-sorcery-rustfmt-hook";
    runtimeInputs = [ rustToolchain ];
    text = ''
      cargo fmt --all -- "$@"
    '';
  };
in
{
  cabal-fmt.enable = true;
  fourmolu = {
    enable = true;
    excludes = [ "bindings/haskell/test/Spec\\.hs" ];
  };
  hlint.enable = true;
  nixfmt.enable = true;
  rustfmt = {
    enable = true;
    entry = "${rustfmtHook}/bin/event-sorcery-rustfmt-hook";
  };
  taplo.enable = true;
  trim-trailing-whitespace.enable = true;
}
