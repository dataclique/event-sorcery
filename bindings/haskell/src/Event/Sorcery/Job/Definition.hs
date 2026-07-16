{-# LANGUAGE AllowAmbiguousTypes #-}

-- | Typed job identities, codecs, and compile-time kind names.
module Event.Sorcery.Job.Definition (
  DeadReason (..),
  Job (..),
  JobDecodeError (..),
  JobId (..),
  jobIdText,
  jobType,
  mkJobId,
) where

import Data.ByteString (ByteString)
import Data.Kind (Type)
import Data.Maybe (Maybe (..))
import Data.Proxy (Proxy (Proxy))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.ULID (ulidFromInteger)
import Data.ULID.Base32 qualified as Base32
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Prelude (Either (..), Eq, Integer, Ord, Show)


-- | Validated ULID identifying one durable job.
newtype JobId = JobId Text
  deriving stock (Eq, Ord, Show)


-- | Domain payload decoding failure.
newtype JobDecodeError = JobDecodeError Text
  deriving stock (Eq, Show)


-- | Terminal reason retained by the engine.
data DeadReason
  = RetriesExhausted
  | Rejected
  | Undecodable
  | Abandoned
  deriving stock (Eq, Show)


-- | Domain contract for a persistable job payload.
class KnownSymbol (JobType job) => Job job where
  type JobType job :: Symbol
  type JobOutput job :: Type
  type JobOutput job = ()
  type JobError job :: Type
  type JobError job = ()


  encodeJob :: job -> ByteString
  decodeJob :: ByteString -> Either JobDecodeError job


-- | Validates canonical ULID text as a job identifier.
mkJobId :: Text -> Maybe JobId
mkJobId value =
  case Base32.decode 26 value :: [(Integer, Text)] of
    [(decoded, remaining)]
      | Text.null remaining ->
          case ulidFromInteger decoded of
            Left _ -> Nothing
            Right _ -> Just (JobId value)
    _ -> Nothing


-- | Returns the canonical text representation of a job identifier.
jobIdText :: JobId -> Text
jobIdText (JobId value) = value


-- | Reflects a job's type-level kind name into text.
jobType :: forall job. Job job => Text
jobType = Text.pack (symbolVal (Proxy @(JobType job)))
