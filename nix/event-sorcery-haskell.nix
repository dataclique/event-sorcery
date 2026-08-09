{
  mkDerivation,
  base,
  bytestring,
  cborg,
  event_sorcery_ffi,
  lib,
  tasty,
  tasty-hunit,
  text,
}:
mkDerivation {
  pname = "event-sorcery";
  version = "0.4.0.0";
  src = bindings/haskell;
  libraryHaskellDepends = [
    base
    bytestring
    cborg
    text
  ];
  librarySystemDepends = [ event_sorcery_ffi ];
  testHaskellDepends = [
    base
    bytestring
    tasty
    tasty-hunit
    text
  ];
  homepage = "https://github.com/dataclique/event-sorcery";
  description = "Type-driven event sourcing over the event-sorcery engine";
  license = lib.meta.getLicenseFromSpdxId "MIT";
}
