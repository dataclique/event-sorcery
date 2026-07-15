module EventSorcery.Job (
  ClaimBudget (..),
  ClaimedJob,
  DeadReason (..),
  JobClaimDetails (..),
  JobClaimReference,
  JobClaimResult (..),
  JobExecutionRoute (..),
  JobId (..),
  JobInstant (..),
  JobKind (..),
  JobLeaseResult (..),
  JobSeed (..),
  JobSettlement (..),
  JobSettlementToken,
  LeaseDuration (..),
  PollLimit (..),
  WorkerId (..),
  acknowledgeJob,
  claimJob,
  commitWithJob,
  deadLetterJob,
  deferJob,
  enqueueJob,
  pollJobs,
  renewJob,
  retryJob,
  settlementToken,
  streamRunnableJobs,
) where

import Codec.CBOR.Decoding (
  Decoder,
  TokenType (TypeNull),
  decodeBytes,
  decodeListLen,
  decodeNull,
  decodeString,
  decodeWord,
  decodeWord32,
  peekTokenType,
 )
import Codec.CBOR.Encoding (
  Encoding,
  encodeBytes,
  encodeInt64,
  encodeListLen,
  encodeString,
  encodeWord,
  encodeWord32,
  encodeWord64,
 )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Conduit (ConduitT, yieldMany)
import Control.Monad (replicateM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT (ExceptT))
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Foldable (foldMap)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (Maybe (..))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Unrestricted.Linear (Ur (Ur))
import Data.Word (Word32, Word64, Word8)
import EventSorcery.Engine (EngineError (BindingProtocolError), Store)
import EventSorcery.Engine.Internal (
  callWithOutput,
  callWithoutOutput,
  withInputBuffer,
  withOpenStore,
 )
import EventSorcery.Engine.Internal.FFI (
  EsBuf,
  EsStore,
  esCommitWithJob,
  esJobAcknowledge,
  esJobClaim,
  esJobDeadLetter,
  esJobDefer,
  esJobEnqueue,
  esJobPoll,
  esJobRenew,
  esJobRetry,
 )
import EventSorcery.Stream (
  ProposedEvent,
  StreamIdentity (..),
  encodeProposedEvent,
 )
import Foreign.C.Types (CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peek, poke)
import Prelude (
  Either (..),
  Eq,
  IO,
  Int,
  Show,
  String,
  Word,
  fail,
  fromIntegral,
  otherwise,
  pure,
  show,
  ($),
  (.),
  (<$),
  (<$>),
  (<>),
  (==),
  (>>=),
 )


newtype JobId = JobId Text
  deriving stock (Eq, Show)


newtype JobKind = JobKind Text
  deriving stock (Eq, Show)


newtype WorkerId = WorkerId Text
  deriving stock (Eq, Show)


newtype JobInstant = JobInstant Int64
  deriving stock (Eq, Show)


newtype LeaseDuration = LeaseDuration Int64
  deriving stock (Eq, Show)


newtype ClaimBudget = ClaimBudget Word32
  deriving stock (Eq, Show)


newtype PollLimit = PollLimit Word32
  deriving stock (Eq, Show)


data JobSeed = JobSeed
  { jobId :: JobId
  , kind :: JobKind
  , payload :: ByteString
  , runAt :: JobInstant
  }
  deriving stock (Eq, Show)


data JobExecutionRoute
  = SubmitExecution
  | ReconcileExecution
  deriving stock (Eq, Show)


newtype JobClaimReference = JobClaimReference ByteString
  deriving stock (Eq, Show)


data JobClaimDetails = JobClaimDetails
  { reference :: JobClaimReference
  , attempt :: Word32
  , route :: JobExecutionRoute
  , payload :: ByteString
  }
  deriving stock (Eq, Show)


data JobLeaseResult
  = LeaseHeld
  | LeaseLost
  deriving stock (Eq, Show)


data JobSettlement
  = SettlementApplied
  | SettlementFenced
  deriving stock (Eq, Show)


newtype JobSettlementToken = JobSettlementToken ByteString
  deriving stock (Eq, Show)


data DeadReason
  = RetriesExhausted
  | Rejected
  | Undecodable
  | Abandoned
  deriving stock (Eq, Show)


data ClaimedJob where
  ClaimedJob
    :: Ur ByteString
    %1 -> ClaimedJob


data JobClaimResult result
  = JobClaimed result
  | JobAbandoned
  | JobContended
  | JobSkipped


commitWithJob
  :: Store
  -> StreamIdentity
  -> Word64
  -> NonEmpty ProposedEvent
  -> JobSeed
  -> IO (Either EngineError ())
commitWithJob store stream expected events seed =
  withOpenStore store $ \handle ->
    withInputBuffer
      (encodeCommitWithJob stream expected events seed)
      (callWithoutOutput . esCommitWithJob handle)


enqueueJob :: Store -> JobSeed -> IO (Either EngineError ())
enqueueJob store seed =
  withOpenStore store $ \handle ->
    withInputBuffer
      (encodeEnqueue seed)
      (callWithoutOutput . esJobEnqueue handle)


pollJobs
  :: Store
  -> JobKind
  -> JobInstant
  -> PollLimit
  -> IO (Either EngineError [JobId])
pollJobs store kind now limit =
  withOpenStore store $ \handle ->
    withInputBuffer (encodePoll kind now limit) $ \request -> do
      response <- callWithOutput (esJobPoll handle request)
      pure (response >>= decodeResponse decodePolledJobs)


streamRunnableJobs
  :: Store
  -> JobKind
  -> JobInstant
  -> PollLimit
  -> ConduitT () JobId (ExceptT EngineError IO) ()
streamRunnableJobs store kind now limit = do
  jobs <- lift (ExceptT (pollJobs store kind now limit))
  yieldMany jobs


claimJob
  :: Store
  -> JobId
  -> WorkerId
  -> JobInstant
  -> LeaseDuration
  -> ClaimBudget
  -> (JobClaimDetails -> ClaimedJob %1 -> Ur result)
  -> IO (Either EngineError (JobClaimResult result))
claimJob store identifier worker now lease budget useClaim =
  withOpenStore store $ \handle ->
    withInputBuffer
      (encodeClaim identifier worker now lease budget)
      (readClaim useClaim . esJobClaim handle)


renewJob
  :: Store
  -> JobClaimReference
  -> JobInstant
  -> IO (Either EngineError JobLeaseResult)
renewJob store (JobClaimReference reference) newLease =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeRenew reference newLease) $ \request ->
      callWithTag
        (esJobRenew handle request)
        decodeLeaseResult


settlementToken :: ClaimedJob %1 -> Ur JobSettlementToken
settlementToken (ClaimedJob (Ur claim)) = Ur (JobSettlementToken claim)


acknowledgeJob
  :: Store
  -> JobSettlementToken
  -> IO (Either EngineError JobSettlement)
acknowledgeJob store (JobSettlementToken claim) =
  settleJob store (encodeAcknowledge claim) esJobAcknowledge


retryJob
  :: Store
  -> JobSettlementToken
  -> JobInstant
  -> Text
  -> IO (Either EngineError JobSettlement)
retryJob
  store
  (JobSettlementToken claim)
  runAt
  failure =
    settleJob store (encodeRetry claim runAt failure) esJobRetry


deferJob
  :: Store
  -> JobSettlementToken
  -> JobInstant
  -> IO (Either EngineError JobSettlement)
deferJob store (JobSettlementToken claim) runAt =
  settleJob store (encodeDefer claim runAt) esJobDefer


deadLetterJob
  :: Store
  -> JobSettlementToken
  -> DeadReason
  -> Text
  -> IO (Either EngineError JobSettlement)
deadLetterJob
  store
  (JobSettlementToken claim)
  reason
  failure =
    settleJob store (encodeDeadLetter claim reason failure) esJobDeadLetter


settleJob
  :: Store
  -> ByteString
  -> (Ptr EsStore -> Ptr EsBuf -> Ptr Word8 -> Ptr EsBuf -> IO CInt)
  -> IO (Either EngineError JobSettlement)
settleJob store requestBytes settle =
  withOpenStore store $ \handle ->
    withInputBuffer requestBytes $ \request ->
      callWithTag
        (settle handle request)
        decodeSettlement


readClaim
  :: (JobClaimDetails -> ClaimedJob %1 -> Ur result)
  -> (Ptr EsBuf -> Ptr EsBuf -> IO CInt)
  -> IO (Either EngineError (JobClaimResult result))
readClaim useClaim call = do
  response <- callWithOutput call

  case response >>= decodeResponse decodeClaimResult of
    Left engineError -> pure (Left engineError)
    Right (DecodedClaim won details) ->
      case useClaim details (ClaimedJob (Ur won)) of
        Ur result -> pure (Right (JobClaimed result))
    Right DecodedAbandoned -> pure (Right JobAbandoned)
    Right DecodedContended -> pure (Right JobContended)
    Right DecodedSkipped -> pure (Right JobSkipped)


callWithTag
  :: (Ptr Word8 -> Ptr EsBuf -> IO CInt)
  -> (Word8 -> Either EngineError value)
  -> IO (Either EngineError value)
callWithTag call decodeTag =
  alloca $ \output -> do
    poke output 255
    called <- callWithoutOutput (call output)

    case called of
      Left engineError -> pure (Left engineError)
      Right () -> decodeTag <$> peek output


encodeCommitWithJob
  :: StreamIdentity
  -> Word64
  -> NonEmpty ProposedEvent
  -> JobSeed
  -> ByteString
encodeCommitWithJob stream expected events seed =
  toStrictByteString $
    encodeListLen 6
      <> encodeWord 1
      <> encodeString stream.aggregateType
      <> encodeString stream.aggregateId
      <> encodeWord64 expected
      <> encodeListLen (fromIntegral (NonEmpty.length events))
      <> foldMap encodeProposedEvent events
      <> encodeListLen 4
      <> encodeSeedFields seed


encodeEnqueue :: JobSeed -> ByteString
encodeEnqueue seed =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeSeedFields seed


encodePoll :: JobKind -> JobInstant -> PollLimit -> ByteString
encodePoll (JobKind kind) (JobInstant now) (PollLimit limit) =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeString kind
      <> encodeInt64 now
      <> encodeWord32 limit


encodeClaim
  :: JobId
  -> WorkerId
  -> JobInstant
  -> LeaseDuration
  -> ClaimBudget
  -> ByteString
encodeClaim
  (JobId identifier)
  (WorkerId worker)
  (JobInstant now)
  (LeaseDuration lease)
  (ClaimBudget budget) =
    toStrictByteString $
      encodeListLen 6
        <> encodeWord 1
        <> encodeString identifier
        <> encodeString worker
        <> encodeInt64 now
        <> encodeInt64 lease
        <> encodeWord32 budget


encodeRenew :: ByteString -> JobInstant -> ByteString
encodeRenew claim (JobInstant newLease) =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeInt64 newLease


encodeAcknowledge :: ByteString -> ByteString
encodeAcknowledge claim =
  toStrictByteString $
    encodeListLen 2
      <> encodeWord 1
      <> encodeBytes claim


encodeRetry :: ByteString -> JobInstant -> Text -> ByteString
encodeRetry claim (JobInstant runAt) failure =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeInt64 runAt
      <> encodeString failure


encodeDefer :: ByteString -> JobInstant -> ByteString
encodeDefer claim (JobInstant runAt) =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeInt64 runAt


encodeDeadLetter :: ByteString -> DeadReason -> Text -> ByteString
encodeDeadLetter claim reason failure =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeWord (deadReasonTag reason)
      <> encodeString failure


encodeSeedFields :: JobSeed -> Encoding
encodeSeedFields seed =
  let JobId identifier = seed.jobId
      JobKind kind = seed.kind
      JobInstant runAt = seed.runAt
   in encodeString identifier
        <> encodeString kind
        <> encodeBytes seed.payload
        <> encodeInt64 runAt


decodePolledJobs :: ByteString -> Either String [JobId]
decodePolledJobs = decodeComplete decodeJobsWire


decodeJobsWire :: Decoder s [JobId]
decodeJobsWire = do
  expectListLength 2
  expectFormatVersion
  count <- decodeListLen
  replicateM count (JobId <$> decodeString)


data DecodedClaim
  = DecodedClaim ByteString JobClaimDetails
  | DecodedAbandoned
  | DecodedContended
  | DecodedSkipped


decodeClaimResult :: ByteString -> Either String DecodedClaim
decodeClaimResult = decodeComplete decodeClaimWire


decodeClaimWire :: Decoder s DecodedClaim
decodeClaimWire = do
  expectListLength 6
  expectFormatVersion
  tag <- decodeWord
  claim <- decodeOptional decodeBytes
  attempt <- decodeOptional decodeWord32
  route <- decodeOptional decodeExecutionRoute
  payload <- decodeOptional decodeBytes

  case (tag, claim, attempt, route, payload) of
    (0, Just won, Just currentAttempt, Just execution, Just body) ->
      let reference = JobClaimReference won
          details = JobClaimDetails reference currentAttempt execution body
       in pure (DecodedClaim won details)
    (1, Nothing, Nothing, Nothing, Nothing) -> pure DecodedAbandoned
    (2, Nothing, Nothing, Nothing, Nothing) -> pure DecodedContended
    (3, Nothing, Nothing, Nothing, Nothing) -> pure DecodedSkipped
    _ -> fail "invalid job-claim response"


decodeExecutionRoute :: Decoder s JobExecutionRoute
decodeExecutionRoute = do
  tag <- decodeWord

  case tag of
    0 -> pure SubmitExecution
    1 -> pure ReconcileExecution
    _ -> fail "invalid job execution route"


decodeOptional :: Decoder s value -> Decoder s (Maybe value)
decodeOptional decoder = do
  token <- peekTokenType

  case token of
    TypeNull -> Nothing <$ decodeNull
    _ -> Just <$> decoder


decodeLeaseResult :: Word8 -> Either EngineError JobLeaseResult
decodeLeaseResult 0 = Right LeaseHeld
decodeLeaseResult 1 = Right LeaseLost
decodeLeaseResult _ = Left (BindingProtocolError "invalid job-lease result")


decodeSettlement :: Word8 -> Either EngineError JobSettlement
decodeSettlement 0 = Right SettlementApplied
decodeSettlement 1 = Right SettlementFenced
decodeSettlement _ = Left (BindingProtocolError "invalid job-settlement result")


deadReasonTag :: DeadReason -> Word
deadReasonTag RetriesExhausted = 0
deadReasonTag Rejected = 1
deadReasonTag Undecodable = 2
deadReasonTag Abandoned = 3


decodeComplete
  :: (forall s. Decoder s value)
  -> ByteString
  -> Either String value
decodeComplete decoder bytes =
  case deserialiseFromBytes decoder (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, value)
      | LazyByteString.null remaining -> Right value
      | otherwise -> Left "trailing bytes after job response"


decodeResponse
  :: (ByteString -> Either String value)
  -> ByteString
  -> Either EngineError value
decodeResponse decoder =
  first (BindingProtocolError . Text.pack) . decoder


expectFormatVersion :: Decoder s ()
expectFormatVersion = do
  version <- decodeWord

  if version == 1
    then pure ()
    else fail "unsupported job format version"


expectListLength :: Int -> Decoder s ()
expectListLength expected = do
  actual <- decodeListLen

  if actual == expected
    then pure ()
    else fail "unexpected CBOR list length"
