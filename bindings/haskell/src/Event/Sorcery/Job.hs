-- | The job feature: durable intents, the claims that run them, and how they
-- settle.
--
-- Everything a consumer needs to enqueue a job, take a claim on it and settle
-- that claim is here, down to the wire codecs the engine and this binding
-- agree on. The engine handle and the errors every call can answer with belong
-- to "Event.Sorcery.Engine" and are re-exported so a job worker needs this one
-- import.
--
-- Committing events together with a job intent belongs to this feature rather
-- than to the stream: what the caller is buying is that the events and the
-- intent land in one transaction, and the intent is the half a job can refuse.
module Event.Sorcery.Job (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ClaimBudget (..),
  ClaimedJob,
  ConflictDetail (..),
  DeadReason (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  Job (..),
  JobClaimDetails (..),
  JobClaimReference (..),
  JobClaimResult (..),
  JobDecodeError (..),
  JobExecutionRoute (..),
  JobId,
  JobInstant (..),
  JobKind (..),
  JobLeaseResult (..),
  JobRefusal (..),
  JobRefusalDetail (..),
  JobSeed (..),
  JobSettlement (..),
  JobSettlementToken,
  LeaseDuration (..),
  PageAdvanceDetail (..),
  PollLimit (..),
  ResourceLimitDetail (..),
  Store,
  WorkerId (..),
  acknowledgeJob,
  claimJob,
  commitWithJob,
  deadLetterJob,
  decodeClaimResult,
  decodePolledJobs,
  deferJob,
  encodeClaim,
  encodeCommitWithJob,
  encodeEnqueue,
  encodePoll,
  encodeRenew,
  enqueueJob,
  jobIdText,
  jobType,
  mkJobId,
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
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Foldable (foldMap)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Text (Text)
import Data.Unrestricted.Linear (Ur (Ur))
import Data.Word (Word32, Word64, Word8)
import Event.Sorcery.Engine.Internal (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  JobId (..),
  JobRefusal (..),
  JobRefusalDetail (..),
  PageAdvanceDetail (..),
  ResourceLimitDetail (..),
  Store,
  callWithOutput,
  callWithTag,
  callWithoutOutput,
  decodeResponse,
  expectListLength,
  withInputBuffer,
  withOpenStore,
 )
import Event.Sorcery.Engine.Internal.FFI (
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
import Event.Sorcery.Job.Definition (
  DeadReason (..),
  Job (..),
  JobDecodeError (..),
  jobIdText,
  jobType,
  mkJobId,
 )
import Event.Sorcery.Stream (
  ProposedEvent,
  StreamIdentity,
  encodeProposedEvent,
  encodeStreamIdentity,
 )
import Foreign.C.Types (CInt)
import Foreign.Ptr (Ptr)
import Prelude (
  Either (..),
  Eq ((==)),
  IO,
  Maybe (Just, Nothing),
  Show (show),
  String,
  Word,
  fail,
  fromIntegral,
  otherwise,
  pure,
  ($),
  (.),
  (<$),
  (<$>),
  (<>),
  (>>=),
 )


newtype JobKind = JobKind Text
  deriving newtype (Eq, Show)


newtype WorkerId = WorkerId Text
  deriving newtype (Eq, Show)


-- | A millisecond instant on the clock the caller keeps for the job queue.
newtype JobInstant = JobInstant Int64
  deriving newtype (Eq, Show)


-- | How long a claim holds its lease, in milliseconds.
newtype LeaseDuration = LeaseDuration Int64
  deriving newtype (Eq, Show)


-- | How many claims one job may be granted before it is abandoned.
newtype ClaimBudget = ClaimBudget Word32
  deriving newtype (Eq, Show)


-- | How many runnable jobs one poll may answer with.
newtype PollLimit = PollLimit Word32
  deriving newtype (Eq, Show)


-- | The durable intent to run one job: what to run, with what, and when.
data JobSeed = JobSeed
  { jobId :: JobId
  , kind :: JobKind
  , payload :: ByteString
  , runAt :: JobInstant
  }
  deriving stock (Eq, Show)


-- | Which body of the job the worker is expected to run for a claim.
--
-- The engine routes the first execution of a job to its submit body and every
-- later one to its reconcile body, so a claim that follows a failed attempt is
-- a reconciliation rather than a fresh submission.
data JobExecutionRoute
  = SubmitExecution
  | ReconcileExecution
  deriving stock (Eq, Show)


-- | The engine-minted handle a claim's lease is renewed against.
newtype JobClaimReference = JobClaimReference ByteString
  deriving newtype (Eq, Show)


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


-- | Whether a settlement took effect or was fenced by a later claim.
data JobSettlement
  = SettlementApplied
  | SettlementFenced
  deriving stock (Eq, Show)


-- | The engine-minted right to settle one claim.
--
-- It is minted by 'settlementToken' out of the claim itself, so a settlement
-- can only be issued by the worker the engine handed the claim to.
newtype JobSettlementToken = JobSettlementToken ByteString
  deriving newtype (Eq, Show)


-- | A won claim, which its worker consumes exactly once.
--
-- The claim is the right to run the job and then settle it, and the engine
-- retains exactly one settleable claim per job. Consuming the value linearly
-- is what keeps a worker from settling the same claim twice.
data ClaimedJob where
  ClaimedJob :: Ur ByteString %1 -> ClaimedJob


-- | What a claim request answered with.
--
-- Only 'JobClaimed' hands the worker anything: the job was abandoned, is
-- already being executed elsewhere, or is not runnable at the requested
-- instant.
data JobClaimResult result
  = JobClaimed result
  | JobAbandoned
  | JobContended
  | JobSkipped
  deriving stock (Eq, Show)


-- | Appends events and the job intent they dispatch in one transaction.
--
-- Either both land or neither does, so a handler cannot leave a job enqueued
-- for events that were never committed.
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


-- | Reads the jobs of one kind that are runnable at the given instant.
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


-- | Yields one poll's runnable jobs into a worker loop.
--
-- The engine bounds a poll by 'PollLimit', so this is one page of candidates
-- rather than the whole queue; a worker drives the loop by polling again at
-- the instant it has reached.
streamRunnableJobs
  :: Store
  -> JobKind
  -> JobInstant
  -> PollLimit
  -> ConduitT () JobId (ExceptT EngineError IO) ()
streamRunnableJobs store kind now limit = do
  jobs <- lift (ExceptT (pollJobs store kind now limit))
  yieldMany jobs


-- | Takes a claim on one job and hands it to the body that runs it.
--
-- The claim is consumed inside 'useClaim' rather than returned, so the worker
-- cannot keep a claim it has already settled.
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


-- | Mints the right to settle a claim out of the claim itself.
settlementToken :: ClaimedJob %1 -> Ur JobSettlementToken
settlementToken (ClaimedJob (Ur claim)) = Ur (JobSettlementToken claim)


-- | Extends a claim's lease, reporting whether the claim still owns the job.
renewJob
  :: Store
  -> JobClaimReference
  -> JobInstant
  -> IO (Either EngineError JobLeaseResult)
renewJob store reference newLease =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeRenew reference newLease) $ \request -> do
      renewed <- callWithTag (esJobRenew handle request)
      pure (renewed >>= decodeLeaseResult)


acknowledgeJob
  :: Store
  -> JobSettlementToken
  -> IO (Either EngineError JobSettlement)
acknowledgeJob store token =
  settleJob store (encodeAcknowledge token) esJobAcknowledge


-- | Records a failed attempt and schedules the next one.
retryJob
  :: Store
  -> JobSettlementToken
  -> JobInstant
  -> Text
  -> IO (Either EngineError JobSettlement)
retryJob store token runAt failure =
  settleJob store (encodeRetry token runAt failure) esJobRetry


-- | Reschedules a claim without spending one of the job's attempts.
deferJob
  :: Store
  -> JobSettlementToken
  -> JobInstant
  -> IO (Either EngineError JobSettlement)
deferJob store token runAt =
  settleJob store (encodeDefer token runAt) esJobDefer


-- | Takes a job out of the queue for good.
deadLetterJob
  :: Store
  -> JobSettlementToken
  -> DeadReason
  -> Text
  -> IO (Either EngineError JobSettlement)
deadLetterJob store token reason failure =
  settleJob store (encodeDeadLetter token reason failure) esJobDeadLetter


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
      <> encodeStreamIdentity stream
      <> encodeWord64 expected
      <> encodeListLen (fromIntegral (NonEmpty.length events))
      <> foldMap encodeProposedEvent events
      <> encodeListLen 4
      <> encodeSeed seed


encodeEnqueue :: JobSeed -> ByteString
encodeEnqueue seed =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeSeed seed


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


encodeRenew :: JobClaimReference -> JobInstant -> ByteString
encodeRenew (JobClaimReference reference) (JobInstant newLease) =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeBytes reference
      <> encodeInt64 newLease


decodePolledJobs :: ByteString -> Either String [JobId]
decodePolledJobs = decodeComplete decodePolledJobsWire


decodeClaimResult
  :: ByteString
  -> Either String (JobClaimResult JobClaimDetails)
decodeClaimResult = decodeComplete decodeClaimWire


settleJob
  :: Store
  -> ByteString
  -> (Ptr (Ptr EsStore) -> Ptr EsBuf -> Ptr Word8 -> Ptr EsBuf -> IO CInt)
  -> IO (Either EngineError JobSettlement)
settleJob store requestBytes settle =
  withOpenStore store $ \handle ->
    withInputBuffer requestBytes $ \request -> do
      settled <- callWithTag (settle handle request)
      pure (settled >>= decodeSettlement)


-- | Runs the claim body against a won claim, and nothing otherwise.
readClaim
  :: (JobClaimDetails -> ClaimedJob %1 -> Ur result)
  -> (Ptr EsBuf -> Ptr EsBuf -> IO CInt)
  -> IO (Either EngineError (JobClaimResult result))
readClaim useClaim call = do
  response <- callWithOutput call

  case response >>= decodeResponse decodeClaimResult of
    Left engineError -> pure (Left engineError)
    Right (JobClaimed details) ->
      let JobClaimReference claim = details.reference
       in case useClaim details (ClaimedJob (Ur claim)) of
            Ur result -> pure (Right (JobClaimed result))
    Right JobAbandoned -> pure (Right JobAbandoned)
    Right JobContended -> pure (Right JobContended)
    Right JobSkipped -> pure (Right JobSkipped)


encodeAcknowledge :: JobSettlementToken -> ByteString
encodeAcknowledge (JobSettlementToken claim) =
  toStrictByteString $
    encodeListLen 2
      <> encodeWord 1
      <> encodeBytes claim


encodeRetry :: JobSettlementToken -> JobInstant -> Text -> ByteString
encodeRetry (JobSettlementToken claim) (JobInstant runAt) failure =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeInt64 runAt
      <> encodeString failure


encodeDefer :: JobSettlementToken -> JobInstant -> ByteString
encodeDefer (JobSettlementToken claim) (JobInstant runAt) =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeInt64 runAt


encodeDeadLetter
  :: JobSettlementToken
  -> DeadReason
  -> Text
  -> ByteString
encodeDeadLetter (JobSettlementToken claim) reason failure =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeBytes claim
      <> encodeWord (deadReasonTag reason)
      <> encodeString failure


encodeSeed :: JobSeed -> Encoding
encodeSeed seed =
  let JobId identifier = seed.jobId
      JobKind kind = seed.kind
      JobInstant runAt = seed.runAt
   in encodeString identifier
        <> encodeString kind
        <> encodeBytes seed.payload
        <> encodeInt64 runAt


deadReasonTag :: DeadReason -> Word
deadReasonTag RetriesExhausted = 0
deadReasonTag Rejected = 1
deadReasonTag Undecodable = 2
deadReasonTag Abandoned = 3


decodePolledJobsWire :: Decoder s [JobId]
decodePolledJobsWire = do
  expectListLength 2
  expectFormatVersion
  count <- decodeListLen
  replicateM count (JobId <$> decodeString)


decodeClaimWire :: Decoder s (JobClaimResult JobClaimDetails)
decodeClaimWire = do
  expectListLength 6
  expectFormatVersion
  tag <- decodeWord
  claim <- decodeOptional (JobClaimReference <$> decodeBytes)
  attempt <- decodeOptional decodeWord32
  route <- decodeOptional decodeExecutionRoute
  payload <- decodeOptional decodeBytes

  case (tag, claim, attempt, route, payload) of
    (0, Just reference, Just attempted, Just execution, Just body) ->
      pure (JobClaimed (JobClaimDetails reference attempted execution body))
    (1, Nothing, Nothing, Nothing, Nothing) -> pure JobAbandoned
    (2, Nothing, Nothing, Nothing, Nothing) -> pure JobContended
    (3, Nothing, Nothing, Nothing, Nothing) -> pure JobSkipped
    _ -> fail "invalid job-claim response"


decodeExecutionRoute :: Decoder s JobExecutionRoute
decodeExecutionRoute = do
  tag <- decodeWord

  case tag of
    0 -> pure SubmitExecution
    1 -> pure ReconcileExecution
    _ -> fail "invalid job execution route"


-- | Reads a field the engine writes only for a won claim.
decodeOptional :: Decoder s value -> Decoder s (Maybe value)
decodeOptional decoder = do
  token <- peekTokenType

  case token of
    TypeNull -> Nothing <$ decodeNull
    _ -> Just <$> decoder


decodeLeaseResult :: Word8 -> Either EngineError JobLeaseResult
decodeLeaseResult 0 = Right LeaseHeld
decodeLeaseResult 1 = Right LeaseLost
decodeLeaseResult tag = Left (BindingProtocolError (UnknownResultTag tag))


decodeSettlement :: Word8 -> Either EngineError JobSettlement
decodeSettlement 0 = Right SettlementApplied
decodeSettlement 1 = Right SettlementFenced
decodeSettlement tag = Left (BindingProtocolError (UnknownResultTag tag))


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


expectFormatVersion :: Decoder s ()
expectFormatVersion = do
  version <- decodeWord

  if version == 1
    then pure ()
    else fail "unsupported job format version"
