module Main (main) where

import Data.ByteString qualified as ByteString
import Data.Either (isLeft)
import Data.Word (Word32)
import EventSorcery.Engine.Codec (
  decodeStoredEvents,
  encodeCommit,
  encodeLoadStream,
  encodeOpenOptions,
 )
import EventSorcery.Engine.Protocol (
  OpenOptions (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Prelude (
  Bool,
  Either (..),
  IO,
  Maybe (Nothing),
  String,
  error,
  pure,
  ($),
  (<>),
  (==),
 )


main :: IO ()
main = do
  assert "open options encoding" $ encodeOpenOptions options == expectedOpen
  assert "load stream encoding" $ encodeLoadStream stream Nothing == expectedLoad
  assert "commit encoding" $ encodeCommit stream 0 [proposed] == expectedCommit
  assert "stored event decoding" $
    decodeStoredEvents stored == Right [expectedStored]
  assert "trailing bytes are rejected" $
    isLeft (decodeStoredEvents (stored <> ByteString.singleton 0))
  assert "unsupported versions are rejected" $
    isLeft (decodeStoredEvents unsupportedVersion)
  assert "top-level arity is enforced" $
    isLeft (decodeStoredEvents wrongTopLevelArity)
  assert "stored-event arity is enforced" $
    isLeft (decodeStoredEvents wrongEventArity)
  assert "opaque payloads must be byte strings" $
    isLeft (decodeStoredEvents arrayPayload)
  where
    options = OpenOptions "sqlite::memory:" 5000 1 (256 :: Word32)
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
        , 25
        , 1
        , 0
        ]
    expectedLoad =
      ByteString.pack
        [132, 1, 103, 97, 99, 99, 111, 117, 110, 116, 99, 111, 110, 101, 246]
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
    unsupportedVersion = ByteString.pack [130, 2, 128]
    wrongTopLevelArity = ByteString.pack [129, 1]
    wrongEventArity =
      ByteString.pack
        [130, 1, 129, 131, 1, 103, 67, 114, 101, 97, 116, 101, 100, 99, 49, 46, 48]
    arrayPayload =
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
        , 130
        , 0
        , 1
        ]


assert :: String -> Bool -> IO ()
assert message condition =
  if condition
    then pure ()
    else error message
