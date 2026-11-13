CREATE TABLE events (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

CREATE TABLE snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
    snapshot_version BIGINT NOT NULL DEFAULT 0 CHECK (snapshot_version >= 0),
    payload JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE TABLE job_queue_projection (
    view_id TEXT NOT NULL PRIMARY KEY,
    version BIGINT NOT NULL CHECK (version > 0),
    payload JSONB NOT NULL,
    lease_until BIGINT,
    kind TEXT GENERATED ALWAYS AS (
        COALESCE(
            payload #>> '{Live,Pending,kind}',
            payload #>> '{Live,Claimed,kind}',
            payload #>> '{Live,Done,kind}',
            payload #>> '{Live,Dead,kind}'
        )
    ) STORED,
    status TEXT GENERATED ALWAYS AS (
        CASE
            WHEN payload #> '{Live,Pending}' IS NOT NULL THEN 'pending'
            WHEN payload #> '{Live,Claimed}' IS NOT NULL THEN 'claimed'
            WHEN payload #> '{Live,Done}' IS NOT NULL THEN 'done'
            WHEN payload #> '{Live,Dead}' IS NOT NULL THEN 'dead'
            ELSE NULL
        END
    ) STORED,
    run_at BIGINT GENERATED ALWAYS AS (
        COALESCE(
            (payload #>> '{Live,Pending,run_at}')::BIGINT,
            (payload #>> '{Live,Claimed,run_at}')::BIGINT
        )
    ) STORED
);

CREATE INDEX job_queue_projection_runnable ON job_queue_projection (kind, status, run_at);
CREATE INDEX job_queue_projection_reclaim ON job_queue_projection (kind, status, lease_until);
