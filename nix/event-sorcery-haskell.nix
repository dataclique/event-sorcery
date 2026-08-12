{
  mkDerivation,
  base,
  bytestring,
  cborg,
  conduit,
  event_sorcery_ffi,
  lib,
  linear-base,
  tasty,
  tasty-hunit,
  text,
  transformers,
}:
mkDerivation {
  pname = "event-sorcery";
  version = "0.4.0.0";
  src = bindings/haskell;
  libraryHaskellDepends = [
    base
    bytestring
    cborg
    conduit
    linear-base
    text
    transformers
  ];
  librarySystemDepends = [ event_sorcery_ffi ];
  testHaskellDepends = [
    base
    bytestring
    conduit
    linear-base
    tasty
    tasty-hunit
    text
    transformers
  ];
  homepage = "https://github.com/dataclique/event-sorcery";
  description = "Type-driven event sourcing over the event-sorcery engine";
  license = lib.meta.getLicenseFromSpdxId "MIT";
}
