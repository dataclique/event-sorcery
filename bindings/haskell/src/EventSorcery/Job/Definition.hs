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
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Prelude (Either, Eq, Ord, Show, otherwise, (==))


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
mkJobId value
  | value == "" = Nothing
  | otherwise = Just (JobId value)


jobIdText :: JobId -> Text
jobIdText (JobId value) = value


jobType :: forall job. Job job => Text
jobType = Text.pack (symbolVal (Proxy @(JobType job)))
