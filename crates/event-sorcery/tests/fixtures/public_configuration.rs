// Consumer fixtures isolate configuration requirements from unrelated trait boilerplate.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::num::ParseIntError;
use std::str::FromStr;

use event_sorcery::{
    CompactionPolicy, DomainEvent, Effect, EventSourced, JobContext, JobFailure, JobOutcome, Label,
    Never, Nil, StandaloneJob,
};

#[derive(Debug, Clone)]
struct ConfigurationId(u64);

impl Display for ConfigurationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(identifier) = self;
        write!(formatter, "{identifier}")
    }
}

impl FromStr for ConfigurationId {
    type Err = ParseIntError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.parse()?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigurationEntity;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum ConfigurationEvent {
    Created,
}

impl DomainEvent for ConfigurationEvent {
    fn event_type(&self) -> String {
        "ConfigurationEvent::Created".to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

macro_rules! entity_configuration {
    ($($configuration:item)*) => {
        #[async_trait]
        impl EventSourced for ConfigurationEntity {
            type Id = ConfigurationId;
            type Error = Never;
            type Command = ();
            type Event = ConfigurationEvent;
            type Materialized = Nil;
            type Jobs = Nil;

            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;
            const AGGREGATE_TYPE: &'static str = "ConfigurationEntity";

            $($configuration)*

            fn originate(_event: &Self::Event) -> Option<Self> {
                Some(Self)
            }

            fn evolve(_entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Self::Error> {
                Ok(Some(Self))
            }

            async fn initialize(_command: ()) -> Result<Effect<Self>, Self::Error> {
                Ok(Effect::Events(Vec::new()))
            }

            async fn transition(&self, _command: ()) -> Result<Effect<Self>, Self::Error> {
                Ok(Effect::Events(Vec::new()))
            }
        }
    };
}

#[derive(Serialize, Deserialize)]
struct ConfigurationJob;

macro_rules! job_configuration {
    ($($configuration:item)*) => {
        impl StandaloneJob for ConfigurationJob {
            type Input = ();
            type Output = ();
            type Error = Never;

            const WORKER_NAME: &'static str = "configuration-job";
            const KIND: &'static str = "configuration-job";

            $($configuration)*

            fn label(&self) -> Label {
                Label::new("configuration-job")
            }

            async fn perform(
                &self,
                _context: &JobContext,
                _input: &(),
            ) -> Result<JobOutcome<()>, JobFailure<Never>> {
                Ok(JobOutcome::Done(()))
            }
        }
    };
}
