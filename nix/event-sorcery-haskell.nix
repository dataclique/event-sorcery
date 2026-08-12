{
  mkDerivation,
  async,
  base,
  bytestring,
  cborg,
  conduit,
  criterion,
  deepseq,
  event_sorcery_ffi,
  lib,
  linear-base,
  tasty,
  tasty-hunit,
  text,
  transformers,
  ulid,
}:
mkDerivation {
  pname = "event-sorcery";
  version = "0.4.0.0";
  src = bindings/haskell;
  libraryHaskellDepends = [
    async
    base
    bytestring
    cborg
    conduit
    linear-base
    text
    transformers
    ulid
  ];
  librarySystemDepends = [ event_sorcery_ffi ];
  testHaskellDepends = [
    async
    base
    bytestring
    conduit
    linear-base
    tasty
    tasty-hunit
    text
    transformers
  ];
  benchmarkHaskellDepends = [
    base
    bytestring
    criterion
    deepseq
    linear-base
    text
  ];
  homepage = "https://github.com/dataclique/event-sorcery";
  description = "Type-driven event sourcing over the event-sorcery engine";
  license = lib.meta.getLicenseFromSpdxId "MIT";
}
