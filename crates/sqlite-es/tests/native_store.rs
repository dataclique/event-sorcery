//! Native SQLite stores, compaction and typed reads.
#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

    use std::{fmt::Display, str::FromStr};

    use event_sorcery::*;
    use sqlite_es::SqliteBackend;

    /// Numeric-only ID that rejects non-numeric strings.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NumericId(u64);

    impl Display for NumericId {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "{}", self.0)
        }
    }

    impl FromStr for NumericId {
        type Err = std::num::ParseIntError;

        fn from_str(value: &str) -> Result<Self, Self::Err> {
            value.parse::<u64>().map(NumericId)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Widget {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum WidgetEvent {
        Created { name: String },
        Renamed { name: String },
    }

    impl DomainEvent for WidgetEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "WidgetEvent::Created".to_string(),
                Self::Renamed { .. } => "WidgetEvent::Renamed".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("widget error")]
    struct WidgetError;

    enum WidgetCommand {
        Create { name: String },
        Rename { name: String },
    }

    #[async_trait]
    impl EventSourced for Widget {
        type Id = NumericId;
        type Error = WidgetError;
        type Command = WidgetCommand;
        type Event = WidgetEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 1;
        const AGGREGATE_TYPE: &'static str = "Widget";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;

        fn originate(event: &WidgetEvent) -> Option<Self> {
            match event {
                WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                WidgetEvent::Renamed { .. } => None,
            }
        }

        fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
            match event {
                WidgetEvent::Created { .. } => Ok(None),
                WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
            }
        }

        async fn initialize(command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Created { name }]))
                }
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }

        async fn transition(&self, command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { .. } => uneventful(),
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }
    }

    /// The same widget under `RetainLast`: compaction keeps the newest
    /// two events per aggregate instead of deleting to the snapshot.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct WindowedWidget<const EVENTS: usize = 2, const SNAPSHOT_SIZE: usize = 1> {
        name: String,
    }

    #[async_trait]
    impl<const EVENTS: usize, const SNAPSHOT_SIZE: usize> EventSourced
        for WindowedWidget<EVENTS, SNAPSHOT_SIZE>
    {
        type Id = NumericId;
        type Error = WidgetError;
        type Command = WidgetCommand;
        type Event = WidgetEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = SNAPSHOT_SIZE;
        const AGGREGATE_TYPE: &'static str = "WindowedWidget";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::RetainLast { events: EVENTS };

        fn originate(event: &WidgetEvent) -> Option<Self> {
            match event {
                WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                WidgetEvent::Renamed { .. } => None,
            }
        }

        fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
            match event {
                WidgetEvent::Created { .. } => Ok(None),
                WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
            }
        }

        async fn initialize(command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Created { name }]))
                }
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }

        async fn transition(&self, command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { .. } => uneventful(),
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }
    }

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    async fn insert_event(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
             event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, 1, 'WidgetEvent::Created', '1.0', ?3, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(r#"{"Created":{"name":"test-widget"}}"#)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_entity_replays_events_without_a_snapshot() {
        type ReplayWidget = WindowedWidget<2, 3>;

        let pool = test_pool().await;
        let backend = SqliteBackend { pool: pool.clone() };
        let store = test_store::<ReplayWidget, _>(&backend);

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "test-widget".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            sqlite_es::load_snapshot(&pool, ReplayWidget::AGGREGATE_TYPE, "42")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            load_entity::<ReplayWidget>(&backend, &NumericId(42))
                .await
                .unwrap(),
            Some(ReplayWidget {
                name: "test-widget".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn standalone_command_persists_the_new_entity() {
        let pool = test_pool().await;
        let backend = SqliteBackend { pool: pool.clone() };
        let id = NumericId(42);

        send_command::<Widget>(
            &backend,
            &id,
            WidgetCommand::Create {
                name: "standalone".to_owned(),
            },
        )
        .await
        .unwrap();

        let store = test_store::<Widget, _>(&SqliteBackend { pool });
        let entity = store.load(&id).await.unwrap().unwrap();
        assert_eq!(entity.name, "standalone");
    }

    #[tokio::test]
    async fn compact_after_snapshot_preserves_suffix_and_uncovered_streams() {
        type PartialSnapshot = WindowedWidget<2, 3>;

        let pool = test_pool().await;
        let backend = SqliteBackend { pool: pool.clone() };
        let store = test_store::<PartialSnapshot, _>(&backend);
        let unrelated = test_store::<Widget, _>(&backend);
        let covered = NumericId(1);
        let uncovered = NumericId(2);
        for id in [&covered, &uncovered] {
            store
                .send(
                    id,
                    WidgetCommand::Create {
                        name: "first".to_owned(),
                    },
                )
                .await
                .unwrap();
        }
        unrelated
            .send(
                &covered,
                WidgetCommand::Create {
                    name: "unrelated".to_owned(),
                },
            )
            .await
            .unwrap();
        for name in ["second", "third", "fourth"] {
            store
                .send(
                    &covered,
                    WidgetCommand::Rename {
                        name: name.to_owned(),
                    },
                )
                .await
                .unwrap();
        }
        let snapshots: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT aggregate_type, aggregate_id, last_sequence
             FROM snapshots ORDER BY aggregate_type, aggregate_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            snapshots,
            [
                ("Widget".to_owned(), "1".to_owned(), 1),
                ("WindowedWidget".to_owned(), "1".to_owned(), 3)
            ]
        );

        let deleted = sqlite_es::compact_events(
            &pool,
            PartialSnapshot::AGGREGATE_TYPE,
            CompactionPolicy::CompactAfterSnapshot,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 3);
        let retained: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT aggregate_type, aggregate_id, sequence
             FROM events ORDER BY aggregate_type, aggregate_id, sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            retained,
            [
                ("Widget".to_owned(), "1".to_owned(), 1),
                ("WindowedWidget".to_owned(), "1".to_owned(), 4),
                ("WindowedWidget".to_owned(), "2".to_owned(), 1)
            ]
        );
        assert_eq!(
            store.load(&covered).await.unwrap(),
            Some(PartialSnapshot {
                name: "fourth".to_owned()
            })
        );
        assert_eq!(
            store.load(&uncovered).await.unwrap(),
            Some(PartialSnapshot {
                name: "first".to_owned()
            })
        );
        assert_eq!(
            unrelated.load(&covered).await.unwrap(),
            Some(Widget {
                name: "unrelated".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn load_entity_uses_snapshot_after_events_are_compacted() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let deleted =
            sqlite_es::compact_events(&pool, Widget::AGGREGATE_TYPE, Widget::COMPACTION_POLICY)
                .await
                .unwrap();

        assert_eq!(deleted, 2);
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'Widget' AND aggregate_id = '42'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count, 0);

        let entity = load_entity::<Widget>(&SqliteBackend { pool: pool.clone() }, &NumericId(42))
            .await
            .unwrap();

        let widget = entity.expect("entity should load from snapshot after compaction");
        assert_eq!(widget.name, "second");
    }

    #[tokio::test]
    async fn commands_still_commit_after_events_are_compacted() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();

        // Compaction leaves the events table reporting a lower maximum sequence
        // than the aggregate's real version; a write must not read that as a
        // concurrency conflict.
        let deleted =
            sqlite_es::compact_events(&pool, Widget::AGGREGATE_TYPE, Widget::COMPACTION_POLICY)
                .await
                .unwrap();
        assert_eq!(deleted, 1);

        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = load_entity::<Widget>(&SqliteBackend { pool: pool.clone() }, &NumericId(42))
            .await
            .unwrap();
        let widget = entity.expect("entity should exist after a post-compaction command");
        assert_eq!(widget.name, "second");
    }

    #[tokio::test]
    async fn oversized_retention_window_is_rejected() {
        let pool = test_pool().await;
        let error = sqlite_es::compact_events(
            &pool,
            <WindowedWidget<{ usize::MAX }>>::AGGREGATE_TYPE,
            <WindowedWidget<{ usize::MAX }>>::COMPACTION_POLICY,
        )
        .await
        .unwrap_err();
        let sqlx::Error::Encode(source) = error else {
            panic!("retention window conversion must report an encoding error");
        };
        source.downcast::<std::num::TryFromIntError>().unwrap();
    }

    #[tokio::test]
    async fn windowed_compaction_preserves_the_unsnapshotted_tail() {
        let pool = test_pool().await;
        let store = test_store::<WindowedWidget<1, 3>, _>(&SqliteBackend { pool: pool.clone() });
        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        for name in ["second", "third", "fourth", "fifth"] {
            store
                .send(
                    &NumericId(7),
                    WidgetCommand::Rename {
                        name: name.to_string(),
                    },
                )
                .await
                .unwrap();
        }

        assert_eq!(
            sqlite_es::compact_events(
                &pool,
                <WindowedWidget<1, 3>>::AGGREGATE_TYPE,
                <WindowedWidget<1, 3>>::COMPACTION_POLICY
            )
            .await
            .unwrap(),
            3
        );
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events WHERE aggregate_type = 'WindowedWidget' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, vec![4, 5]);
        let entity = load_entity::<WindowedWidget<1, 3>>(
            &SqliteBackend { pool: pool.clone() },
            &NumericId(7),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(entity.name, "fifth");
    }

    #[tokio::test]
    async fn retain_last_keeps_the_newest_window_and_the_entity_loads() {
        let pool = test_pool().await;
        let store = test_store::<WindowedWidget, _>(&SqliteBackend { pool: pool.clone() });

        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        for renamed in ["second", "third", "fourth", "fifth"] {
            store
                .send(
                    &NumericId(7),
                    WidgetCommand::Rename {
                        name: renamed.to_string(),
                    },
                )
                .await
                .unwrap();
        }

        let deleted = sqlite_es::compact_events(
            &pool,
            WindowedWidget::<2, 1>::AGGREGATE_TYPE,
            WindowedWidget::<2, 1>::COMPACTION_POLICY,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 3, "five events minus the two-event window");
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events \
             WHERE aggregate_type = 'WindowedWidget' AND aggregate_id = '7' \
             ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, vec![4, 5], "the newest window survives");

        let entity =
            load_entity::<WindowedWidget>(&SqliteBackend { pool: pool.clone() }, &NumericId(7))
                .await
                .unwrap()
                .expect("the entity loads from snapshot plus the retained window");
        assert_eq!(entity.name, "fifth");
    }

    #[tokio::test]
    async fn windowed_compaction_isolates_streams_and_preserves_unsnapshotted_events() {
        let pool = test_pool().await;
        let windowed = test_store::<WindowedWidget<2, 5>, _>(&SqliteBackend { pool: pool.clone() });
        let unrelated = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });

        for (aggregate_id, event_count) in [(7, 6), (8, 11), (9, 4)] {
            windowed
                .send(
                    &NumericId(aggregate_id),
                    WidgetCommand::Create {
                        name: "initial".to_string(),
                    },
                )
                .await
                .unwrap();

            for sequence in 2..=event_count {
                windowed
                    .send(
                        &NumericId(aggregate_id),
                        WidgetCommand::Rename {
                            name: format!("version-{sequence}"),
                        },
                    )
                    .await
                    .unwrap();
            }
        }

        unrelated
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "unrelated".to_string(),
                },
            )
            .await
            .unwrap();

        for sequence in 2..=12 {
            unrelated
                .send(
                    &NumericId(7),
                    WidgetCommand::Rename {
                        name: format!("unrelated-{sequence}"),
                    },
                )
                .await
                .unwrap();
        }

        let snapshots: Vec<(String, i64)> = sqlx::query_as(
            "SELECT aggregate_id, last_sequence FROM snapshots \
             WHERE aggregate_type = 'WindowedWidget' ORDER BY aggregate_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(snapshots, vec![("7".to_string(), 5), ("8".to_string(), 10)]);

        assert_eq!(
            sqlite_es::compact_events(
                &pool,
                <WindowedWidget<2, 5>>::AGGREGATE_TYPE,
                <WindowedWidget<2, 5>>::COMPACTION_POLICY
            )
            .await
            .unwrap(),
            13,
        );

        for (aggregate_type, aggregate_id, expected) in [
            ("WindowedWidget", "7", vec![5, 6]),
            ("WindowedWidget", "8", vec![10, 11]),
            ("WindowedWidget", "9", vec![1, 2, 3, 4]),
            ("Widget", "7", (1..=12).collect()),
        ] {
            let remaining: Vec<i64> = sqlx::query_scalar(
                "SELECT sequence FROM events \
                 WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
                 ORDER BY sequence",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .fetch_all(&pool)
            .await
            .unwrap();

            assert_eq!(remaining, expected, "{aggregate_type}/{aggregate_id}");
        }
    }

    #[tokio::test]
    async fn snapshot_version_advances_across_loads() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let snapshot_version: i64 = sqlx::query_scalar(
            "SELECT snapshot_version FROM snapshots \
             WHERE aggregate_type = 'Widget' AND aggregate_id = '42'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(snapshot_version, 2);
    }

    #[tokio::test]
    async fn snapshot_rebuild_applies_events_exactly_once() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct Tally {
            count: u64,
        }

        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        enum TallyEvent {
            Started,
            Incremented,
        }

        impl DomainEvent for TallyEvent {
            fn event_type(&self) -> String {
                format!("{self:?}")
            }

            fn event_version(&self) -> String {
                "1.0".to_string()
            }
        }

        enum TallyCommand {
            Start,
            Increment,
        }

        #[async_trait]
        impl EventSourced for Tally {
            type Id = NumericId;
            type Error = WidgetError;
            type Command = TallyCommand;
            type Event = TallyEvent;
            type Materialized = Nil;
            type Jobs = Nil;

            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;
            const SNAPSHOT_SIZE: usize = 1;
            const AGGREGATE_TYPE: &'static str = "Tally";
            const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

            fn originate(event: &TallyEvent) -> Option<Self> {
                match event {
                    TallyEvent::Started => Some(Self { count: 0 }),
                    TallyEvent::Incremented => None,
                }
            }

            fn evolve(entity: &Self, event: &TallyEvent) -> Result<Option<Self>, WidgetError> {
                match event {
                    TallyEvent::Started => Ok(None),
                    TallyEvent::Incremented => Ok(Some(Self {
                        count: entity.count + 1,
                    })),
                }
            }

            async fn initialize(command: TallyCommand) -> Result<Effect<Self>, WidgetError> {
                match command {
                    TallyCommand::Start => Ok(Effect::Events(vec![TallyEvent::Started])),
                    TallyCommand::Increment => Ok(Effect::Events(vec![TallyEvent::Incremented])),
                }
            }

            async fn transition(&self, command: TallyCommand) -> Result<Effect<Self>, WidgetError> {
                match command {
                    TallyCommand::Start => uneventful(),
                    TallyCommand::Increment => Ok(Effect::Events(vec![TallyEvent::Incremented])),
                }
            }
        }

        let pool = test_pool().await;
        let store = test_store::<Tally, _>(&SqliteBackend { pool: pool.clone() });

        // SNAPSHOT_SIZE = 1 forces commit's snapshot rebuild
        // (`update_snapshot_with_events`) after every command, exercising the
        // re-apply path that requires `Lifecycle::handle` to leave `self` at
        // its pre-command state. A counting entity makes double-application
        // visible as a wrong count rather than a coincidentally-equal value.
        store
            .send(&NumericId(1), TallyCommand::Start)
            .await
            .unwrap();
        store
            .send(&NumericId(1), TallyCommand::Increment)
            .await
            .unwrap();
        store
            .send(&NumericId(1), TallyCommand::Increment)
            .await
            .unwrap();

        let entity = load_entity::<Tally>(&SqliteBackend { pool: pool.clone() }, &NumericId(1))
            .await
            .unwrap();
        let tally = entity.expect("tally should exist after three commands");
        assert_eq!(tally.count, 2);

        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM snapshots \
             WHERE aggregate_type = 'Tally' AND aggregate_id = '1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(snapshot, serde_json::json!({"Live": {"count": 2}}));
    }

    #[tokio::test]
    async fn retain_policy_does_not_compact_events() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct RetainedWidget {
            name: String,
        }

        #[async_trait]
        impl EventSourced for RetainedWidget {
            type Id = NumericId;
            type Error = WidgetError;
            type Command = WidgetCommand;
            type Event = WidgetEvent;
            type Materialized = Nil;
            type Jobs = Nil;

            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;
            const SNAPSHOT_SIZE: usize = 1;
            const AGGREGATE_TYPE: &'static str = "RetainedWidget";
            const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

            fn originate(event: &WidgetEvent) -> Option<Self> {
                match event {
                    WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                    WidgetEvent::Renamed { .. } => None,
                }
            }

            fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
                match event {
                    WidgetEvent::Created { .. } => Ok(None),
                    WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
                }
            }

            async fn initialize(command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
                match command {
                    WidgetCommand::Create { name } => {
                        Ok(Effect::Events(vec![WidgetEvent::Created { name }]))
                    }
                    WidgetCommand::Rename { name } => {
                        Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                    }
                }
            }

            async fn transition(
                &self,
                command: WidgetCommand,
            ) -> Result<Effect<Self>, WidgetError> {
                match command {
                    WidgetCommand::Create { .. } => uneventful(),
                    WidgetCommand::Rename { name } => {
                        Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                    }
                }
            }
        }

        let pool = test_pool().await;
        let store = test_store::<RetainedWidget, _>(&SqliteBackend { pool: pool.clone() });
        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "kept".to_string(),
                },
            )
            .await
            .unwrap();

        let snapshot = sqlite_es::load_snapshot(&pool, "RetainedWidget", "7")
            .await
            .unwrap()
            .expect("the store wrote a snapshot covering the first event");
        assert_eq!(snapshot.current_sequence, 1);

        let deleted = sqlite_es::compact_events(
            &pool,
            RetainedWidget::AGGREGATE_TYPE,
            RetainedWidget::COMPACTION_POLICY,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 0);
        let sequences: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence
             FROM events
             WHERE aggregate_type = 'RetainedWidget' AND aggregate_id = '7'
             ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(sequences, [1]);
    }

    #[tokio::test]
    async fn load_entity_returns_none_when_no_events() {
        let pool = test_pool().await;

        let entity = load_entity::<Widget>(&SqliteBackend { pool: pool.clone() }, &NumericId(999))
            .await
            .unwrap();

        assert!(entity.is_none(), "expected None for nonexistent aggregate");
    }

    #[tokio::test]
    async fn load_all_ids_returns_parsed_ids() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "10").await;
        insert_event(&pool, "Widget", "20").await;

        let ids = sqlite_es::load_all_ids::<NumericId>(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();

        assert_eq!(ids, vec![NumericId(10), NumericId(20)]);
    }

    #[tokio::test]
    async fn aggregate_ids_deduplicate_event_and_snapshot_rows_before_sorting_and_counting() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });

        for id in [2, 10, 3] {
            store
                .send(
                    &NumericId(id),
                    WidgetCommand::Create {
                        name: id.to_string(),
                    },
                )
                .await
                .unwrap();
        }
        for id in ["2", "10", "3"] {
            let snapshot = sqlite_es::load_snapshot(&pool, Widget::AGGREGATE_TYPE, id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.current_sequence, 1);
        }

        let ids = sqlite_es::load_all_ids::<NumericId>(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();
        let count = sqlite_es::count_aggregates(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();

        assert_eq!(ids, [NumericId(10), NumericId(2), NumericId(3)]);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn load_ids_paginated_orders_by_maximum_rowid_after_an_append() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });
        for id in [1, 2] {
            store
                .send(
                    &NumericId(id),
                    WidgetCommand::Create {
                        name: id.to_string(),
                    },
                )
                .await
                .unwrap();
        }
        store
            .send(
                &NumericId(1),
                WidgetCommand::Rename {
                    name: "renamed".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 1, 0)
                .await
                .unwrap(),
            [NumericId(1)]
        );
        assert_eq!(
            sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 1, 1)
                .await
                .unwrap(),
            [NumericId(2)]
        );
        assert_eq!(
            sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 1, 2)
                .await
                .unwrap(),
            []
        );
    }

    #[tokio::test]
    async fn paginated_ids_break_event_and_snapshot_rowid_ties_by_id() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });
        for id in [10, 2] {
            store
                .send(
                    &NumericId(id),
                    WidgetCommand::Create {
                        name: id.to_string(),
                    },
                )
                .await
                .unwrap();
        }
        let stream = StreamIdentity::new(Widget::AGGREGATE_TYPE, "2");
        sqlite_es::Engine::new(pool.clone())
            .discard_snapshot(&stream)
            .await
            .unwrap();
        assert_eq!(
            sqlite_es::compact_events(&pool, Widget::AGGREGATE_TYPE, Widget::COMPACTION_POLICY)
                .await
                .unwrap(),
            1
        );
        // The tables have independent rowids; make the surviving entries tie.
        let event_rowid: i64 =
            sqlx::query_scalar("SELECT rowid FROM events WHERE aggregate_id = '2'")
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query("UPDATE snapshots SET rowid = ? WHERE aggregate_id = '10'")
            .bind(event_rowid)
            .execute(&pool)
            .await
            .unwrap();

        let first = sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 1, 0)
            .await
            .unwrap();
        let second =
            sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 1, 1)
                .await
                .unwrap();

        assert_eq!(first, [NumericId(10)]);
        assert_eq!(second, [NumericId(2)]);
    }

    #[tokio::test]
    async fn load_all_ids_includes_snapshot_only_aggregates() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });
        store
            .send(
                &NumericId(30),
                WidgetCommand::Create {
                    name: "snapshot-only".to_string(),
                },
            )
            .await
            .unwrap();
        sqlite_es::compact_events(&pool, Widget::AGGREGATE_TYPE, Widget::COMPACTION_POLICY)
            .await
            .unwrap();

        let ids = sqlite_es::load_all_ids::<NumericId>(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();

        assert_eq!(ids, vec![NumericId(30)]);
    }

    #[tokio::test]
    async fn paginated_ids_use_descending_snapshot_rowids_for_snapshot_only_streams() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });
        for id in [3, 12, 2] {
            store
                .send(
                    &NumericId(id),
                    WidgetCommand::Create {
                        name: id.to_string(),
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            sqlite_es::compact_events(&pool, Widget::AGGREGATE_TYPE, Widget::COMPACTION_POLICY)
                .await
                .unwrap(),
            3
        );

        let first = sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 2, 0)
            .await
            .unwrap();
        let second =
            sqlite_es::load_ids_paginated::<NumericId>(&pool, Widget::AGGREGATE_TYPE, 2, 2)
                .await
                .unwrap();

        assert_eq!(first, [NumericId(2), NumericId(12)]);
        assert_eq!(second, [NumericId(3)]);
    }

    #[tokio::test]
    async fn load_all_ids_returns_empty_when_no_events() {
        let pool = test_pool().await;

        let ids = sqlite_es::load_all_ids::<NumericId>(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();

        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn load_all_ids_errors_on_unparseable_id() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "42").await;
        insert_event(&pool, "Widget", "not-a-number").await;

        let error = sqlite_es::load_all_ids::<NumericId>(&pool, Widget::AGGREGATE_TYPE)
            .await
            .expect_err("should fail when an ID cannot parse");

        match error {
            sqlite_es::LoadAllIdsError::InvalidIds {
                aggregate_type,
                ids,
            } => {
                assert_eq!(aggregate_type, "Widget");
                assert_eq!(ids, vec!["not-a-number"]);
            }
            sqlite_es::LoadAllIdsError::Sql(sql_error) => {
                panic!("expected InvalidIds, got Sql: {sql_error}")
            }
            sqlite_es::LoadAllIdsError::NumericConversion(conv_error) => {
                panic!("expected InvalidIds, got NumericConversion: {conv_error}")
            }
        }
    }

    #[tokio::test]
    async fn load_all_ids_ignores_other_aggregate_types() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "1").await;
        insert_event(&pool, "OtherAggregate", "should-be-excluded").await;

        let ids = sqlite_es::load_all_ids::<NumericId>(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();

        assert_eq!(ids, vec![NumericId(1)]);
    }

    #[tokio::test]
    async fn count_aggregates_includes_snapshot_only_aggregates() {
        let pool = test_pool().await;
        let store = test_store::<Widget, _>(&SqliteBackend { pool: pool.clone() });
        store
            .send(
                &NumericId(30),
                WidgetCommand::Create {
                    name: "snapshot-only".to_string(),
                },
            )
            .await
            .unwrap();
        sqlite_es::compact_events(&pool, Widget::AGGREGATE_TYPE, Widget::COMPACTION_POLICY)
            .await
            .unwrap();

        let count = sqlite_es::count_aggregates(&pool, Widget::AGGREGATE_TYPE)
            .await
            .unwrap();

        assert_eq!(count, 1);
    }
}
