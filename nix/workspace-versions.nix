{
  expectedVersion,
  derivationVersions,
  workspaceNames,
  lockedPackages,
}:
let
  members = builtins.filter (
    package: builtins.elem package.name workspaceNames && !(package ? source)
  ) lockedPackages;
  memberNames = map (package: package.name) members;
in
builtins.sort builtins.lessThan memberNames == builtins.sort builtins.lessThan workspaceNames
&& builtins.all (package: package.version == expectedVersion) members
&& builtins.all (version: version == expectedVersion) derivationVersions
