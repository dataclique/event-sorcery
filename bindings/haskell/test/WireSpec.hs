module WireSpec (spec) where

import Control.Monad (unless)
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.Either (Either (Left, Right))
import Data.List (isInfixOf)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Word (Word64)
import Event.Sorcery.Engine (
  ConflictDetail (..),
  EngineError (..),
  OpenOptions (..),
  ResourceLimitDetail (..),
  decodeCloseStatus,
  decodeEngineError,
  encodeOpenOptions,
 )
import Event.Sorcery.Job (
  ClaimBudget (..),
  JobClaimDetails (..),
  JobClaimReference (..),
  JobClaimResult (JobAbandoned, JobClaimed),
  JobExecutionRoute (ReconcileExecution),
  JobId,
  JobInstant (..),
  JobKind (..),
  JobRefusal (InvalidClaimPolicy, UnrecognizedRefusal),
  JobRefusalDetail (..),
  JobSeed (..),
  LeaseDuration (..),
  PollLimit (..),
  WorkerId (..),
  decodeClaimResult,
  decodePolledJobs,
  encodeClaim,
  encodeCommitWithJob,
  encodeEnqueue,
  encodePoll,
  encodeRenew,
  mkJobId,
 )
import Event.Sorcery.Stream (
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  EventType (..),
  EventVersion (..),
  PageAdvanceDetail (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  nextCursor,
 )
import Test.Hspec (
  Expectation,
  Spec,
  describe,
  expectationFailure,
  it,
  runIO,
  shouldBe,
 )
import Prelude (
  IO,
  Maybe (..),
  String,
  error,
  lines,
  map,
  read,
  readFile,
  words,
  ($),
  (.),
  (<$>),
  (<>),
  (==),
 )


spec :: Spec
spec = describe "engine wire format" $ do
  describe "encoding" $ do
    it "open options" $
      encodeOpenOptions options `shouldBe` expectedOpen

    it "load stream without a cursor" $
      encodeLoadStream stream Nothing `shouldBe` expectedLoadWithoutCursor

    it "load stream after a cursor" $
      encodeLoadStream stream (Just 256) `shouldBe` expectedLoadAfterCursor

    it "current version" $
      encodeCurrentVersion stream `shouldBe` expectedCurrentVersion

    it "commit" $
      encodeCommit stream 0 [proposed] `shouldBe` expectedCommit

    it "commit carrying a job intent" $
      encodeCommitWithJob stream 0 (proposed :| []) seed
        `shouldBe` expectedCommitWithJob

    it "job enqueue" $
      encodeEnqueue seed `shouldBe` expectedEnqueue

    it "job poll" $
      encodePoll kind (JobInstant 5) (PollLimit 10) `shouldBe` expectedPoll

    it "job claim" $
      encodeClaim
        wireJobId
        (WorkerId "worker")
        (JobInstant 5)
        (LeaseDuration 30_000)
        (ClaimBudget 50)
        `shouldBe` expectedClaim

    it "claim renewal" $
      encodeRenew reference (JobInstant 60_000) `shouldBe` expectedRenew

  describe "decoding" $ do
    it "stored event" $
      decodeStoredEvents stored `shouldBe` Right [expectedStored]

    it "malformed-input detail" $
      decodeEngineError 1 malformedInput `shouldBe` Right MalformedInput

    it "storage-failure detail" $
      decodeEngineError 4 storageFailure
        `shouldBe` Right (StorageFailure "storage failure")

    it "invalid-state detail" $
      decodeEngineError 5 invalidState
        `shouldBe` Right (InvalidState "store is closed")

    it "keeps the numeric identity of an unmodelled code" $
      decodeEngineError 42 unmodelledCode
        `shouldBe` Right (UnknownEngineError 42)

    it "job-refusal detail" $
      decodeEngineError 3 jobRefusal
        `shouldBe` Right
          ( JobRefused
              (JobRefusalDetail wireJobId InvalidClaimPolicy)
          )

    it "keeps the identity of an unmodelled refusal" $
      decodeEngineError 3 unmodelledRefusal
        `shouldBe` Right
          ( JobRefused
              ( JobRefusalDetail
                  wireJobId
                  (UnrecognizedRefusal "later reason")
              )
          )

    it "conflict detail" $
      decodeEngineError 2 conflict
        `shouldBe` Right
          ( OptimisticConflict
              ( ConflictDetail
                  (AggregateType "account")
                  (AggregateId "one")
                  0
                  1
              )
          )

    it "resource-limit detail" $
      decodeEngineError 6 resourceLimit
        `shouldBe` Right
          ( ResourceLimitExceeded
              (ResourceLimitDetail "payload" 65 64)
          )

    it "panic detail" $
      decodeEngineError 100 enginePanic `shouldBe` Right EnginePanic

    it "rejects disagreement between status and encoded code" $
      assertDecodeFailure
        "engine status does not match encoded error code"
        (decodeEngineError 4 conflict)

    it "rejects trailing bytes" $
      decodeStoredEvents (stored <> ByteString.singleton 0)
        `shouldBe` Left "trailing bytes after stored events"

    it "rejects unsupported versions" $
      assertDecodeFailure
        "unsupported stored-events format version"
        (decodeStoredEvents unsupportedVersion)

    it "enforces top-level arity" $
      assertDecodeFailure
        "unexpected CBOR list length"
        (decodeStoredEvents wrongTopLevelArity)

    it "enforces stored-event arity" $
      assertDecodeFailure
        "unexpected CBOR list length"
        (decodeStoredEvents wrongEventArity)

    it "requires byte-string payloads" $
      assertDecodeFailure
        "expected bytes"
        (decodeStoredEvents arrayPayload)

    it "polled jobs" $
      decodePolledJobs polledJobs `shouldBe` Right [wireJobId]

    it "won claim" $
      decodeClaimResult wonClaim
        `shouldBe` Right
          ( JobClaimed
              ( JobClaimDetails
                  reference
                  3
                  ReconcileExecution
                  (ByteString.pack [0, 1])
              )
          )

    it "claim on an abandoned job" $
      decodeClaimResult abandonedClaim `shouldBe` Right JobAbandoned

    it "rejects unsupported job versions" $
      assertDecodeFailure
        "unsupported job format version"
        (decodePolledJobs unsupportedJobVersion)

  describe "stream paging" $ do
    it "starts a fresh walk at the last sequence of the page" $
      nextCursor Nothing (pageEndingAt 4096) `shouldBe` Right 4096

    it "advances to the last sequence the page reached" $
      nextCursor (Just 4096) (pageEndingAt 4097) `shouldBe` Right 4097

    it "refuses a page that ends on the cursor" $
      nextCursor (Just 4096) (pageEndingAt 4096)
        `shouldBe` Left
          ( BindingProtocolError
              (PageDidNotAdvance (PageAdvanceDetail 4096 4096))
          )

    it "refuses a page that ends behind the cursor" $
      nextCursor (Just 4096) (pageEndingAt 4095)
        `shouldBe` Left
          ( BindingProtocolError
              (PageDidNotAdvance (PageAdvanceDetail 4096 4095))
          )

  describe "detail-free close statuses" $ do
    it "success" $
      decodeCloseStatus 0 `shouldBe` Right ()

    it "rejected owner handle" $
      decodeCloseStatus 5
        `shouldBe` Left (InvalidState "store close rejected the owner handle")

    it "panic" $
      decodeCloseStatus 100 `shouldBe` Left EnginePanic

    it "unmodelled status" $
      decodeCloseStatus 42 `shouldBe` Left (UnknownEngineError 42)

  conformance


-- | Drives the corpus both bindings share through this binding's codecs.
--
-- The fixtures above pin the shape this binding expects on its own terms. The
-- corpus is the same bytes the Rust ABI asserts against, so a boundary change
-- only one side made is a failure here rather than a runtime disagreement.
conformance :: Spec
conformance = do
  vectors <- runIO loadVectors

  let vector name = conformanceVector name vectors

  describe "shared encoding conformance" $ do
    it "open options" $
      encodeOpenOptions options `shouldBe` vector "open-options"

    it "load stream without a cursor" $
      encodeLoadStream stream Nothing `shouldBe` vector "load-stream"

    it "current version" $
      encodeCurrentVersion stream `shouldBe` vector "current-version"

    it "commit" $
      encodeCommit stream 0 [proposed] `shouldBe` vector "commit"

    it "stored events" $
      decodeStoredEvents (vector "stored-events")
        `shouldBe` Right [expectedStored]

    it "conflict error" $
      decodeEngineError 2 (vector "conflict-error")
        `shouldBe` Right
          ( OptimisticConflict
              ( ConflictDetail
                  (AggregateType "account")
                  (AggregateId "one")
                  0
                  1
              )
          )


-- | Pins a negative case to the rule it was written for.
--
-- The decoders report failures as text, and a fixture that is both malformed
-- and truncated fails for either reason, so only the codec's own message shows
-- that the intended check is still the one doing the rejecting.
assertDecodeFailure :: String -> Either String value -> Expectation
assertDecodeFailure expected outcome =
  case outcome of
    Left message ->
      unless (expected `isInfixOf` message) $
        expectationFailure
          ("expected a failure mentioning " <> expected <> ", got " <> message)
    Right _ ->
      expectationFailure ("expected a failure mentioning " <> expected)


-- | Reads the corpus from the package root Cabal runs test suites from.
loadVectors :: IO [(String, ByteString)]
loadVectors =
  map decodeVector . lines <$> readFile "conformance/encoding-v1.vectors"


-- | Takes one corpus line as a vector name and its decimal octets.
decodeVector :: String -> (String, ByteString)
decodeVector encoded = case words encoded of
  [] -> error "empty conformance vector"
  name : octets -> (name, ByteString.pack (map read octets))


-- | Joins every line the corpus records under one vector name.
conformanceVector :: String -> [(String, ByteString)] -> ByteString
conformanceVector name vectors =
  case [octets | (vectorName, octets) <- vectors, vectorName == name] of
    [] -> error ("missing conformance vector: " <> name)
    chunks -> ByteString.concat chunks


options :: OpenOptions
options = OpenOptions "sqlite::memory:" 5000 1 256


stream :: StreamIdentity
stream = StreamIdentity (AggregateType "account") (AggregateId "one")


proposed :: ProposedEvent
proposed =
  ProposedEvent
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])


kind :: JobKind
kind = JobKind "email"


wireJobId :: JobId
wireJobId = case mkJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV" of
  Just identifier -> identifier
  Nothing -> error "wire fixture job identifier was rejected"


seed :: JobSeed
seed =
  JobSeed
    wireJobId
    kind
    (ByteString.pack [0, 1])
    (JobInstant 5)


reference :: JobClaimReference
reference = JobClaimReference (ByteString.pack [7, 8])


-- | A two-event page, so the cursor has to come from the last event.
pageEndingAt :: Word64 -> NonEmpty StoredEvent
pageEndingAt lastSequence = pageEvent 1 :| [pageEvent lastSequence]


pageEvent :: Word64 -> StoredEvent
pageEvent sequenceNumber =
  StoredEvent
    sequenceNumber
    (EventType "Created")
    (EventVersion "1.0")
    ByteString.empty


expectedStored :: StoredEvent
expectedStored =
  StoredEvent
    1
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])


expectedOpen :: ByteString
expectedOpen =
  ByteString.pack
    [ 133 -- array(5)
    , 1 -- format version 1
    , 111 -- text(15)
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
    , 58 -- sqlite::memory:
    , 25
    , 19
    , 136 -- uint16(5000)
    , 1 -- pool size 1
    , 25
    , 1
    , 0 -- uint16(256) runtime threads
    ]


expectedLoadWithoutCursor :: ByteString
expectedLoadWithoutCursor =
  ByteString.pack
    [ 132 -- array(4)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 246 -- null cursor
    ]


expectedLoadAfterCursor :: ByteString
expectedLoadAfterCursor =
  ByteString.pack
    [ 132 -- array(4)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 25
    , 1
    , 0 -- uint16(256) cursor
    ]


expectedCurrentVersion :: ByteString
expectedCurrentVersion =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    ]


expectedCommit :: ByteString
expectedCommit =
  ByteString.pack
    [ 133 -- array(5)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 0 -- expected version 0
    , 129 -- array(1) proposed event
    , 131 -- array(3) event product
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 66
    , 0
    , 1 -- bytes(2)
    ]


expectedCommitWithJob :: ByteString
expectedCommitWithJob =
  ByteString.pack
    [ 134 -- array(6)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 0 -- expected version 0
    , 129 -- array(1) proposed event
    , 131 -- array(3) event product
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 66
    , 0
    , 1 -- bytes(2)
    , 132 -- array(4) job seed
    , 120
    , 26
    , 48
    , 49
    , 65
    , 82
    , 90
    , 51
    , 78
    , 68
    , 69
    , 75
    , 84
    , 83
    , 86
    , 52
    , 82
    , 82
    , 70
    , 70
    , 81
    , 54
    , 57
    , 71
    , 53
    , 70
    , 65
    , 86 -- text(26) 01ARZ3NDEKTSV4RRFFQ69G5FAV
    , 101
    , 101
    , 109
    , 97
    , 105
    , 108 -- text(5) email
    , 66
    , 0
    , 1 -- bytes(2)
    , 5 -- run at 5
    ]


expectedEnqueue :: ByteString
expectedEnqueue =
  ByteString.pack
    [ 133 -- array(5)
    , 1 -- format version 1
    , 120
    , 26
    , 48
    , 49
    , 65
    , 82
    , 90
    , 51
    , 78
    , 68
    , 69
    , 75
    , 84
    , 83
    , 86
    , 52
    , 82
    , 82
    , 70
    , 70
    , 81
    , 54
    , 57
    , 71
    , 53
    , 70
    , 65
    , 86 -- text(26) 01ARZ3NDEKTSV4RRFFQ69G5FAV
    , 101
    , 101
    , 109
    , 97
    , 105
    , 108 -- text(5) email
    , 66
    , 0
    , 1 -- bytes(2)
    , 5 -- run at 5
    ]


expectedPoll :: ByteString
expectedPoll =
  ByteString.pack
    [ 132 -- array(4)
    , 1 -- format version 1
    , 101
    , 101
    , 109
    , 97
    , 105
    , 108 -- text(5) email
    , 5 -- now 5
    , 10 -- limit 10
    ]


expectedClaim :: ByteString
expectedClaim =
  ByteString.pack
    [ 134 -- array(6)
    , 1 -- format version 1
    , 120
    , 26
    , 48
    , 49
    , 65
    , 82
    , 90
    , 51
    , 78
    , 68
    , 69
    , 75
    , 84
    , 83
    , 86
    , 52
    , 82
    , 82
    , 70
    , 70
    , 81
    , 54
    , 57
    , 71
    , 53
    , 70
    , 65
    , 86 -- text(26) 01ARZ3NDEKTSV4RRFFQ69G5FAV
    , 102
    , 119
    , 111
    , 114
    , 107
    , 101
    , 114 -- text(6) worker
    , 5 -- now 5
    , 25
    , 117
    , 48 -- uint16(30000) lease duration
    , 24
    , 50 -- uint8(50) claim budget
    ]


expectedRenew :: ByteString
expectedRenew =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 66
    , 7
    , 8 -- bytes(2) claim reference
    , 25
    , 234
    , 96 -- uint16(60000) new lease instant
    ]


polledJobs :: ByteString
polledJobs =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) runnable job
    , 120
    , 26
    , 48
    , 49
    , 65
    , 82
    , 90
    , 51
    , 78
    , 68
    , 69
    , 75
    , 84
    , 83
    , 86
    , 52
    , 82
    , 82
    , 70
    , 70
    , 81
    , 54
    , 57
    , 71
    , 53
    , 70
    , 65
    , 86 -- text(26) 01ARZ3NDEKTSV4RRFFQ69G5FAV
    ]


wonClaim :: ByteString
wonClaim =
  ByteString.pack
    [ 134 -- array(6)
    , 1 -- format version 1
    , 0 -- claimed
    , 66
    , 7
    , 8 -- bytes(2) claim reference
    , 3 -- attempt 3
    , 1 -- reconcile execution
    , 66
    , 0
    , 1 -- bytes(2) payload
    ]


-- | A claim on a job the queue has given up on, so every field is absent.
abandonedClaim :: ByteString
abandonedClaim =
  ByteString.pack
    [ 134 -- array(6)
    , 1 -- format version 1
    , 1 -- abandoned
    , 246
    , 246
    , 246
    , 246 -- null claim, attempt, route and payload
    ]


unsupportedJobVersion :: ByteString
unsupportedJobVersion =
  ByteString.pack
    [ 130 -- array(2)
    , 2 -- unsupported format version 2
    , 128 -- array(0) runnable jobs
    ]


jobRefusal :: ByteString
jobRefusal =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 3 -- job-refusal error
    , 130 -- array(2) refusal detail
    , 120
    , 26
    , 48
    , 49
    , 65
    , 82
    , 90
    , 51
    , 78
    , 68
    , 69
    , 75
    , 84
    , 83
    , 86
    , 52
    , 82
    , 82
    , 70
    , 70
    , 81
    , 54
    , 57
    , 71
    , 53
    , 70
    , 65
    , 86 -- text(26) 01ARZ3NDEKTSV4RRFFQ69G5FAV
    , 120
    , 24
    , 105
    , 110
    , 118
    , 97
    , 108
    , 105
    , 100
    , 32
    , 106
    , 111
    , 98
    , 32
    , 99
    , 108
    , 97
    , 105
    , 109
    , 32
    , 112
    , 111
    , 108
    , 105
    , 99
    , 121 -- text(24) invalid job claim policy
    ]


-- | A refusal reason a newer engine renders and this binding has no leaf for.
unmodelledRefusal :: ByteString
unmodelledRefusal =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 3 -- job-refusal error
    , 130 -- array(2) refusal detail
    , 120
    , 26
    , 48
    , 49
    , 65
    , 82
    , 90
    , 51
    , 78
    , 68
    , 69
    , 75
    , 84
    , 83
    , 86
    , 52
    , 82
    , 82
    , 70
    , 70
    , 81
    , 54
    , 57
    , 71
    , 53
    , 70
    , 65
    , 86 -- text(26) 01ARZ3NDEKTSV4RRFFQ69G5FAV
    , 108
    , 108
    , 97
    , 116
    , 101
    , 114
    , 32
    , 114
    , 101
    , 97
    , 115
    , 111
    , 110 -- text(12) later reason
    ]


stored :: ByteString
stored =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) stored event
    , 132 -- array(4) event product
    , 1 -- sequence 1
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 66
    , 0
    , 1 -- bytes(2)
    ]


unsupportedVersion :: ByteString
unsupportedVersion =
  ByteString.pack
    [ 130 -- array(2)
    , 2 -- unsupported format version 2
    , 128 -- array(0) stored events
    ]


wrongTopLevelArity :: ByteString
wrongTopLevelArity =
  ByteString.pack
    [ 129 -- array(1), expected array(2)
    , 1 -- format version 1
    ]


wrongEventArity :: ByteString
wrongEventArity =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) stored event
    , 131 -- array(3), expected array(4)
    , 1 -- sequence 1
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    ]


arrayPayload :: ByteString
arrayPayload =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) stored event
    , 132 -- array(4) event product
    , 1 -- sequence 1
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 130
    , 0
    , 1 -- array(2), not bytes(2)
    ]


conflict :: ByteString
conflict =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 2 -- conflict error
    , 132 -- array(4) conflict detail
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 0 -- expected version 0
    , 1 -- actual version 1
    ]


malformedInput :: ByteString
malformedInput =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 1 -- malformed-input error
    , 111
    , 109
    , 97
    , 108
    , 102
    , 111
    , 114
    , 109
    , 101
    , 100
    , 32
    , 105
    , 110
    , 112
    , 117
    , 116 -- text(15) malformed input
    ]


storageFailure :: ByteString
storageFailure =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 4 -- storage-failure error
    , 111
    , 115
    , 116
    , 111
    , 114
    , 97
    , 103
    , 101
    , 32
    , 102
    , 97
    , 105
    , 108
    , 117
    , 114
    , 101 -- text(15) storage failure
    ]


invalidState :: ByteString
invalidState =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 5 -- invalid-state error
    , 111
    , 115
    , 116
    , 111
    , 114
    , 101
    , 32
    , 105
    , 115
    , 32
    , 99
    , 108
    , 111
    , 115
    , 101
    , 100 -- text(15) store is closed
    ]


-- | An error class the engine does not define, carrying an arbitrary detail.
--
-- The catch-all has to survive a detail whose shape the binding has never
-- seen, so the fixture pairs an undefined class with a product the decoder
-- has no clause for.
unmodelledCode :: ByteString
unmodelledCode =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 24
    , 42 -- an error class the engine does not define
    , 130 -- array(2) detail of an unknown shape
    , 98
    , 105
    , 100 -- text(2) id
    , 100
    , 108
    , 97
    , 116
    , 101 -- text(4) late
    ]


resourceLimit :: ByteString
resourceLimit =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 6 -- resource-limit error
    , 131 -- array(3) resource detail
    , 103
    , 112
    , 97
    , 121
    , 108
    , 111
    , 97
    , 100 -- text(7) payload
    , 24
    , 65 -- observed 65
    , 24
    , 64 -- limit 64
    ]


enginePanic :: ByteString
enginePanic =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 24
    , 100 -- panic error
    , 246 -- null detail
    ]
