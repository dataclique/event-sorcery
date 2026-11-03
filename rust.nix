{ pkgs, craneLib }:

let
  version = (fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
  src = pkgs.lib.cleanSourceWith {
    src = ./.;
    filter =
      path: type:
      craneLib.filterCargoSources path type
      || pkgs.lib.hasPrefix (toString ./crates/sqlite-es/migrations) path
      || pkgs.lib.hasPrefix (toString ./.sqlx) path
      || pkgs.lib.hasPrefix (toString ./bindings/haskell/conformance) path;
  };
  depsSrc = pkgs.lib.cleanSourceWith {
    src = ./.;
    filter =
      path: type:
      type == "directory"
      || builtins.elem (baseNameOf path) [
        "Cargo.toml"
        "Cargo.lock"
      ];
  };
  cargoVendorDir = craneLib.vendorCargoDeps {
    src = depsSrc;
    cargoLock = ./Cargo.lock;
  };
  commonArgs = {
    pname = "event-sorcery-workspace";
    inherit version src cargoVendorDir;
    cargoExtraArgs = "--locked --workspace --all-features";
    nativeBuildInputs = [ pkgs.pkg-config ];
    buildInputs = [
      pkgs.openssl
      pkgs.sqlite
    ];
    SQLX_OFFLINE = "true";
    CARGO_INCREMENTAL = "0";
    meta = {
      description = "Event Sorcery Rust workspace";
      license = pkgs.lib.licenses.mit;
      platforms = pkgs.lib.platforms.unix;
    };
  };
  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { src = depsSrc; });
in
{
  package = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoBuildExtraArgs = "--all-targets";
    }
  );

  fmt = craneLib.cargoFmt {
    inherit (commonArgs) pname version src;
    cargoExtraArgs = "--all";
  };

  test = craneLib.cargoNextest (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoNextestExtraArgs = "--all-targets";
    }
  );

  clippy = craneLib.cargoClippy (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoClippyExtraArgs = "--all-targets -- -D warnings";
    }
  );

  engine = craneLib.buildPackage (
    commonArgs
    // {
      pname = "event-sorcery-ffi";
      inherit cargoArtifacts;
      cargoExtraArgs = "--locked --package event-sorcery-ffi";
      postInstall = ''
        mkdir -p $out/include
        cp target/release/build/event-sorcery-ffi-*/out/event_sorcery.h $out/include/event_sorcery.h
      '';
      doInstallCheck = true;
      installCheckPhase = ''
        runHook preInstallCheck
        test -s $out/lib/libevent_sorcery_ffi.a
        test -s $out/include/event_sorcery.h
        runHook postInstallCheck
      '';
      meta = commonArgs.meta // {
        description = "event-sorcery FFI engine: static library and generated C header";
      };
    }
  );
}
