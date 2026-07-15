module Main (main) where

import Data.ByteString qualified as ByteString
import EventSorcery.Engine.Codec (
  decodeEngineError,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  encodeOpenOptions,
 )
import EventSorcery.Engine.Types (
  EngineError (..),
  ErrorClass (ConflictError),
  OpenOptions (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Prelude (Either (..), IO, Maybe (Nothing), error, pure, (&&), (==))


main :: IO ()
main =
  if encodeOpenOptions options == expectedOpen
    && encodeLoadStream stream Nothing == expectedLoad
    && encodeCurrentVersion stream == expectedCurrentVersion
    && encodeCommit stream 0 [proposed] == expectedCommit
    && decodeStoredEvents stored == Right [expectedStored]
    && decodeEngineError conflict
      == Right (EngineError ConflictError "optimistic conflict")
    then pure ()
    else error "engine codecs did not match the deterministic CBOR vectors"
  where
    options = OpenOptions "sqlite::memory:" 5000 1 1
    stream = StreamIdentity "account" "one"
    proposed = ProposedEvent "Created" "1.0" (ByteString.pack [0, 1])
    expectedStored = StoredEvent 1 "Created" "1.0" (ByteString.pack [0, 1])
    expectedOpen =
      ByteString.pack
        [ 133
        , 1
        , 111
        , 115
        , 113
        , 108
        , 105
        , 116
        , 101
        , 58
        , 58
        , 109
        , 101
        , 109
        , 111
        , 114
        , 121
        , 58
        , 25
        , 19
        , 136
        , 1
        , 1
        ]
    expectedLoad =
      ByteString.pack
        [132, 1, 103, 97, 99, 99, 111, 117, 110, 116, 99, 111, 110, 101, 246]
    expectedCurrentVersion =
      ByteString.pack
        [131, 1, 103, 97, 99, 99, 111, 117, 110, 116, 99, 111, 110, 101]
    expectedCommit =
      ByteString.pack
        [ 133
        , 1
        , 103
        , 97
        , 99
        , 99
        , 111
        , 117
        , 110
        , 116
        , 99
        , 111
        , 110
        , 101
        , 0
        , 129
        , 131
        , 103
        , 67
        , 114
        , 101
        , 97
        , 116
        , 101
        , 100
        , 99
        , 49
        , 46
        , 48
        , 66
        , 0
        , 1
        ]
    stored =
      ByteString.pack
        [ 130
        , 1
        , 129
        , 132
        , 1
        , 103
        , 67
        , 114
        , 101
        , 97
        , 116
        , 101
        , 100
        , 99
        , 49
        , 46
        , 48
        , 66
        , 0
        , 1
        ]
    conflict =
      ByteString.pack
        [ 131
        , 1
        , 2
        , 115
        , 111
        , 112
        , 116
        , 105
        , 109
        , 105
        , 115
        , 116
        , 105
        , 99
        , 32
        , 99
        , 111
        , 110
        , 102
        , 108
        , 105
        , 99
        , 116
        ]
