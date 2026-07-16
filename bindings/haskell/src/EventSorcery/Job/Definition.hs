{-# LANGUAGE AllowAmbiguousTypes #-}

module EventSorcery.Job.Definition (
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


newtype JobId = JobId Text
  deriving stock (Eq, Ord, Show)


newtype JobDecodeError = JobDecodeError Text
  deriving stock (Eq, Show)


data DeadReason
  = RetriesExhausted
  | Rejected
  | Undecodable
  | Abandoned
  deriving stock (Eq, Show)


class KnownSymbol (JobType job) => Job job where
  type JobType job :: Symbol
  type JobOutput job :: Type
  type JobOutput job = ()
  type JobError job :: Type
  type JobError job = ()


  encodeJob :: job -> ByteString
  decodeJob :: ByteString -> Either JobDecodeError job


mkJobId :: Text -> Maybe JobId
mkJobId value =
  case Base32.decode 26 value :: [(Integer, Text)] of
    [(decoded, remaining)]
      | Text.null remaining ->
          case ulidFromInteger decoded of
            Left _ -> Nothing
            Right _ -> Just (JobId value)
    _ -> Nothing


jobIdText :: JobId -> Text
jobIdText (JobId value) = value


jobType :: forall job. Job job => Text
jobType = Text.pack (symbolVal (Proxy @(JobType job)))
