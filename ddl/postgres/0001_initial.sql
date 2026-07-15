CREATE TABLE events (
  global_offset  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  aggregate_type TEXT   NOT NULL,
  aggregate_id   TEXT   NOT NULL,
  sequence       BIGINT NOT NULL,
  event_type     TEXT   NOT NULL,
  event_version  BIGINT NOT NULL,
  payload        BYTEA  NOT NULL,
  UNIQUE (aggregate_type, aggregate_id, sequence)
);

CREATE TABLE jobs (
  job_id        TEXT PRIMARY KEY,
  record        BYTEA NOT NULL,
  kind          TEXT NOT NULL,
  status        TEXT NOT NULL,
  run_at        BIGINT,
  lease_expires BIGINT
);

CREATE INDEX jobs_poll ON jobs (kind, status, run_at, lease_expires);

CREATE TABLE outbox (
  delivery_id   BYTEA PRIMARY KEY,
  record        BYTEA NOT NULL,
  status        TEXT NOT NULL,
  run_at        BIGINT,
  lease_expires BIGINT
);

CREATE INDEX outbox_poll ON outbox (status, run_at, lease_expires);

CREATE TABLE delivery_receipts (
  delivery_id BYTEA PRIMARY KEY
);

CREATE TABLE reactors (
  name         TEXT PRIMARY KEY,
  event_offset BIGINT NOT NULL
);

CREATE TABLE projections (
  name         TEXT PRIMARY KEY,
  event_offset BIGINT NOT NULL,
  view         BYTEA NOT NULL
);

CREATE TABLE snapshots (
  aggregate_type TEXT   NOT NULL,
  aggregate_id   TEXT   NOT NULL,
  sequence       BIGINT NOT NULL,
  schema_version BIGINT NOT NULL,
  history        TEXT   NOT NULL CHECK (history IN ('retained', 'compacted')),
  payload        BYTEA  NOT NULL,
  PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE TABLE schemas (
  kind           TEXT   NOT NULL CHECK (kind IN ('aggregate', 'projection')),
  name           TEXT   NOT NULL,
  schema_version BIGINT NOT NULL,
  PRIMARY KEY (kind, name)
);
