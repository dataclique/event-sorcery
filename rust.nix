{ pkgs, craneLib }:

let
  version = (fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
  src = pkgs.lib.cleanSourceWith {
    src = ./.;
    filter =
      path: type:
      craneLib.filterCargoSources path type
      || pkgs.lib.hasPrefix (toString ./crates/sqlite-es/migrations) path
      || pkgs.lib.hasPrefix (toString ./crates/postgr-es/migrations) path
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
  testCargoArgs = commonArgs.cargoExtraArgs;

  postgresTestArgs = pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
    nativeCheckInputs = [
      pkgs.postgresql
      pkgs.postgresqlTestHook
    ];
    postgresqlTestUserOptions = "LOGIN CREATEDB";
    postgresqlTestSetupPost = ''
      export DATABASE_URL="postgresql://test_user@localhost/test_db?host=$PGHOST"
      export PGPASSWORD=""
      export PGPASSFILE=/dev/null
      export PGSSLMODE=disable
    '';
  };
  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { src = depsSrc; });
in
{
  package = craneLib.buildPackage (
    commonArgs
    // postgresTestArgs
    // {
      inherit cargoArtifacts;
      cargoExtraArgs = testCargoArgs;
      cargoBuildExtraArgs = "--all-targets";
      preBuild = ''
        for package in event-sorcery postgr-es; do
          echo "Checking backend isolation for $package"
          cargo check ''${CARGO_PROFILE:+--profile $CARGO_PROFILE} \
            --locked --offline --package "$package" --lib --no-default-features
          cargo check ''${CARGO_PROFILE:+--profile $CARGO_PROFILE} \
            --locked --offline --package "$package" --lib
        done

        for package in event-sorcery-core event-sorcery sqlite-es postgr-es; do
          case "$package" in
            event-sorcery-core|event-sorcery)
              forbidden='sqlite-es|postgr-es|postgres-es|sqlx-sqlite|sqlx-postgres|libsqlite3-sys'
              ;;
            sqlite-es) forbidden='postgr-es|postgres-es|sqlx-postgres' ;;
            postgr-es) forbidden='sqlite-es|sqlx-sqlite|libsqlite3-sys' ;;
          esac

          graph="$TMPDIR/$package-dependencies"
          cargo tree --locked --offline --package "$package" --all-features \
            --edges normal,dev,build --prefix none --format '{p}' > "$graph"
          if grep -E "^($forbidden) v" "$graph"; then
            echo "Unexpected database adapter or driver in $package dependencies" >&2
            exit 1
          fi
        done
      '';
    }
  );

  fmt = craneLib.cargoFmt {
    inherit (commonArgs) pname version src;
    cargoExtraArgs = "--all";
  };

  test = craneLib.cargoNextest (
    commonArgs
    // postgresTestArgs
    // {
      inherit cargoArtifacts;
      cargoExtraArgs = testCargoArgs;
      cargoNextestExtraArgs = "--all-targets --no-fail-fast";
      postCheck = ''
        cargo test ''${CARGO_PROFILE:+--profile $CARGO_PROFILE} \
          ${testCargoArgs} --doc
      '';
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
      nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ pkgs.nushell ];
      postInstall = ''
        nu ${./nix/install-ffi-header.nu} \
          "$cargoBuildLog" "$(cargo pkgid --package event-sorcery-ffi)" "$out"
      '';
      doInstallCheck = true;
      installCheckPhase = ''
        runHook preInstallCheck
        nu ${./nix/install-ffi-header.test.nu} ${./nix/install-ffi-header.nu} "$TMPDIR/ffi-header-tests"
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
