module Main (main) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.Either (Either (Left, Right))
import Data.List (isInfixOf)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Word (Word64)
import EventSorcery.Engine (
  ConflictDetail (..),
  EngineError (..),
  OpenOptions (..),
  ResourceLimitDetail (..),
  decodeCloseStatus,
  decodeEngineError,
  encodeOpenOptions,
 )
import EventSorcery.Job (
  ClaimBudget (..),
  JobClaimDetails (..),
  JobClaimReference (..),
  JobClaimResult (JobAbandoned, JobClaimed),
  JobExecutionRoute (ReconcileExecution),
  JobId (..),
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
 )
import EventSorcery.Stream (
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
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, (@?=))
import Prelude (IO, Maybe (..), String, ($), (<>))


main :: IO ()
main = defaultMain tests


tests :: TestTree
tests =
  testGroup
    "engine wire format"
    [ testGroup
        "encoding"
        [ testCase "open options" $
            encodeOpenOptions options @?= expectedOpen
        , testCase "load stream without a cursor" $
            encodeLoadStream stream Nothing @?= expectedLoadWithoutCursor
        , testCase "load stream after a cursor" $
            encodeLoadStream stream (Just 256) @?= expectedLoadAfterCursor
        , testCase "current version" $
            encodeCurrentVersion stream @?= expectedCurrentVersion
        , testCase "commit" $
            encodeCommit stream 0 [proposed] @?= expectedCommit
        , testCase "commit carrying a job intent" $
            encodeCommitWithJob stream 0 (proposed :| []) seed
              @?= expectedCommitWithJob
        , testCase "job enqueue" $
            encodeEnqueue seed @?= expectedEnqueue
        , testCase "job poll" $
            encodePoll kind (JobInstant 5) (PollLimit 10) @?= expectedPoll
        , testCase "job claim" $
            encodeClaim
              (JobId "job")
              (WorkerId "worker")
              (JobInstant 5)
              (LeaseDuration 30_000)
              (ClaimBudget 50)
              @?= expectedClaim
        , testCase "claim renewal" $
            encodeRenew reference (JobInstant 60_000) @?= expectedRenew
        ]
    , testGroup
        "decoding"
        [ testCase "stored event" $
            decodeStoredEvents stored @?= Right [expectedStored]
        , testCase "malformed-input detail" $
            decodeEngineError 1 malformedInput @?= Right MalformedInput
        , testCase "storage-failure detail" $
            decodeEngineError 4 storageFailure
              @?= Right (StorageFailure "storage failure")
        , testCase "invalid-state detail" $
            decodeEngineError 5 invalidState
              @?= Right (InvalidState "store is closed")
        , testCase "keeps the numeric identity of an unmodelled code" $
            decodeEngineError 42 unmodelledCode
              @?= Right (UnknownEngineError 42)
        , testCase "job-refusal detail" $
            decodeEngineError 3 jobRefusal
              @?= Right
                ( JobRefused
                    (JobRefusalDetail (JobId "job") InvalidClaimPolicy)
                )
        , testCase "keeps the identity of an unmodelled refusal" $
            decodeEngineError 3 unmodelledRefusal
              @?= Right
                ( JobRefused
                    ( JobRefusalDetail
                        (JobId "job")
                        (UnrecognizedRefusal "later reason")
                    )
                )
        , testCase "conflict detail" $
            decodeEngineError 2 conflict
              @?= Right
                ( OptimisticConflict
                    ( ConflictDetail
                        (AggregateType "account")
                        (AggregateId "one")
                        0
                        1
                    )
                )
        , testCase "resource-limit detail" $
            decodeEngineError 6 resourceLimit
              @?= Right
                ( ResourceLimitExceeded
                    (ResourceLimitDetail "payload" 65 64)
                )
        , testCase "panic detail" $
            decodeEngineError 100 enginePanic @?= Right EnginePanic
        , testCase "rejects disagreement between status and encoded code" $
            assertDecodeFailure
              "engine status does not match encoded error code"
              (decodeEngineError 4 conflict)
        , testCase "rejects trailing bytes" $
            decodeStoredEvents (stored <> ByteString.singleton 0)
              @?= Left "trailing bytes after stored events"
        , testCase "rejects unsupported versions" $
            assertDecodeFailure
              "unsupported stored-events format version"
              (decodeStoredEvents unsupportedVersion)
        , testCase "enforces top-level arity" $
            assertDecodeFailure
              "unexpected CBOR list length"
              (decodeStoredEvents wrongTopLevelArity)
        , testCase "enforces stored-event arity" $
            assertDecodeFailure
              "unexpected CBOR list length"
              (decodeStoredEvents wrongEventArity)
        , testCase "requires byte-string payloads" $
            assertDecodeFailure "expected bytes" (decodeStoredEvents arrayPayload)
        , testCase "polled jobs" $
            decodePolledJobs polledJobs @?= Right [JobId "job"]
        , testCase "won claim" $
            decodeClaimResult wonClaim
              @?= Right
                ( JobClaimed
                    ( JobClaimDetails
                        reference
                        3
                        ReconcileExecution
                        (ByteString.pack [0, 1])
                    )
                )
        , testCase "claim on an abandoned job" $
            decodeClaimResult abandonedClaim @?= Right JobAbandoned
        , testCase "rejects unsupported job versions" $
            assertDecodeFailure
              "unsupported job format version"
              (decodePolledJobs unsupportedJobVersion)
        ]
    , testGroup
        "stream paging"
        [ testCase "starts a fresh walk at the last sequence of the page" $
            nextCursor Nothing (pageEndingAt 4096) @?= Right 4096
        , testCase "advances to the last sequence the page reached" $
            nextCursor (Just 4096) (pageEndingAt 4097) @?= Right 4097
        , testCase "refuses a page that ends on the cursor" $
            nextCursor (Just 4096) (pageEndingAt 4096)
              @?= Left
                ( BindingProtocolError
                    (PageDidNotAdvance (PageAdvanceDetail 4096 4096))
                )
        , testCase "refuses a page that ends behind the cursor" $
            nextCursor (Just 4096) (pageEndingAt 4095)
              @?= Left
                ( BindingProtocolError
                    (PageDidNotAdvance (PageAdvanceDetail 4096 4095))
                )
        ]
    , testGroup
        "detail-free close statuses"
        [ testCase "success" $
            decodeCloseStatus 0 @?= Right ()
        , testCase "rejected owner handle" $
            decodeCloseStatus 5
              @?= Left (InvalidState "store close rejected the owner handle")
        , testCase "panic" $
            decodeCloseStatus 100 @?= Left EnginePanic
        , testCase "unmodelled status" $
            decodeCloseStatus 42 @?= Left (UnknownEngineError 42)
        ]
    ]


-- | Pins a negative case to the rule it was written for.
--
-- The decoders report failures as text, and a fixture that is both malformed
-- and truncated fails for either reason, so only the codec's own message shows
-- that the intended check is still the one doing the rejecting.
assertDecodeFailure :: String -> Either String value -> Assertion
assertDecodeFailure expected outcome =
  case outcome of
    Left message ->
      assertBool
        ("expected a failure mentioning " <> expected <> ", got " <> message)
        (expected `isInfixOf` message)
    Right _ -> assertFailure ("expected a failure mentioning " <> expected)


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


seed :: JobSeed
seed =
  JobSeed
    (JobId "job")
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
    , 99
    , 106
    , 111
    , 98 -- text(3) job
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
    , 99
    , 106
    , 111
    , 98 -- text(3) job
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
    , 99
    , 106
    , 111
    , 98 -- text(3) job
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
    , 99
    , 106
    , 111
    , 98 -- text(3) job
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
    , 99
    , 106
    , 111
    , 98 -- text(3) job
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
    , 99
    , 106
    , 111
    , 98 -- text(3) job
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
