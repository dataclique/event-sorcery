let
  check = import ./workspace-versions.nix;
  expectedVersion = (fromTOML (builtins.readFile ../Cargo.toml)).workspace.package.version;
  workspaceNames = [
    "sqlite-es"
    "event-sorcery"
    "event-sorcery-ffi"
  ];
  lockedPackages = (fromTOML (builtins.readFile ../Cargo.lock)).package;
  arguments = {
    inherit expectedVersion workspaceNames lockedPackages;
    derivationVersions = [ expectedVersion ];
  };
  differentVersion = "${expectedVersion}-mismatch";
in
{
  acceptsCurrentWorkspace =
    assert check arguments;
    true;

  rejectsEffectiveMemberMismatch =
    assert builtins.all (
      memberName:
      !(check (
        arguments
        // {
          lockedPackages = map (
            package:
            if package.name == memberName && !(package ? source) then
              package // { version = differentVersion; }
            else
              package
          ) lockedPackages;
        }
      ))
    ) workspaceNames;
    true;

  ignoresRegistryNameCollision =
    assert check (
      arguments
      // {
        lockedPackages = lockedPackages ++ [
          {
            name = "sqlite-es";
            version = differentVersion;
            source = "registry+https://github.com/rust-lang/crates.io-index";
          }
        ];
      }
    );
    true;

  ignoresOtherLocalPackages =
    assert check (
      arguments
      // {
        lockedPackages = lockedPackages ++ [
          {
            name = "event-sorcery-ffi-helper";
            version = differentVersion;
          }
        ];
      }
    );
    true;

  rejectsMissingMember =
    assert
      !(check (
        arguments
        // {
          lockedPackages = builtins.filter (
            package: package.name != "sqlite-es" || package ? source
          ) lockedPackages;
        }
      ));
    true;

  rejectsDuplicateLocalMember =
    assert
      !(check (
        arguments
        // {
          lockedPackages = lockedPackages ++ [
            {
              name = "sqlite-es";
              version = expectedVersion;
            }
          ];
        }
      ));
    true;

  rejectsIncorrectDerivationLabel =
    assert !(check (arguments // { derivationVersions = [ differentVersion ]; }));
    true;
}
