module Main (main) where

import Data.ByteString qualified as ByteString
import Event.Sorcery.Engine (
  EngineError (..),
  ErrorClass (ConflictError),
  OpenOptions (..),
  decodeEngineError,
  encodeOpenOptions,
 )
import Event.Sorcery.Stream (
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
 )
import Paths_event_sorcery (getDataFileName)
import Prelude (
  Either (..),
  IO,
  Maybe (Nothing),
  String,
  error,
  lines,
  map,
  pure,
  read,
  readFile,
  words,
  (&&),
  (.),
  (<$>),
  (==),
 )


main :: IO ()
main = do
  vectors <- loadVectors
  let expectedOpen = conformanceVector "open-options" vectors
      expectedLoad = conformanceVector "load-stream" vectors
      expectedCurrentVersion = conformanceVector "current-version" vectors
      expectedCommit = conformanceVector "commit" vectors
      stored = conformanceVector "stored-events" vectors
      conflict = conformanceVector "conflict-error" vectors
      options = OpenOptions "sqlite::memory:" 5000 1 1
      stream = StreamIdentity "account" "one"
      proposed = ProposedEvent "Created" "1.0" (ByteString.pack [0, 1])
      expectedStored = StoredEvent 1 "Created" "1.0" (ByteString.pack [0, 1])

  if encodeOpenOptions options == expectedOpen
    && encodeLoadStream stream Nothing == expectedLoad
    && encodeCurrentVersion stream == expectedCurrentVersion
    && encodeCommit stream 0 [proposed] == expectedCommit
    && decodeStoredEvents stored == Right [expectedStored]
    && decodeEngineError conflict
      == Right (EngineError ConflictError "optimistic conflict")
    then pure ()
    else error "engine codecs did not match the shared CBOR corpus"


loadVectors :: IO [(String, ByteString.ByteString)]
loadVectors = do
  path <- getDataFileName "conformance/encoding-v1.vectors"
  map decodeVector . lines <$> readFile path


decodeVector :: String -> (String, ByteString.ByteString)
decodeVector encoded = case words encoded of
  [] -> error "empty conformance vector"
  name : bytes -> (name, ByteString.pack (map read bytes))


conformanceVector
  :: String
  -> [(String, ByteString.ByteString)]
  -> ByteString.ByteString
conformanceVector name vectors =
  case [bytes | (vectorName, bytes) <- vectors, vectorName == name] of
    [] -> error "missing conformance vector"
    chunks -> ByteString.concat chunks
