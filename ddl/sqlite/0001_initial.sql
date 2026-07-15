CREATE TABLE events (
  global_offset  INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_type TEXT    NOT NULL,
  aggregate_id   TEXT    NOT NULL,
  sequence       INTEGER NOT NULL,
  event_type     TEXT    NOT NULL,
  event_version  INTEGER NOT NULL,
  payload        BLOB    NOT NULL,
  UNIQUE (aggregate_type, aggregate_id, sequence)
) STRICT;

CREATE TABLE jobs (
  job_id        TEXT PRIMARY KEY,
  record        BLOB NOT NULL,
  kind          TEXT NOT NULL,
  status        TEXT NOT NULL,
  run_at        INTEGER,
  lease_expires INTEGER
) STRICT;

CREATE INDEX jobs_poll ON jobs (kind, status, run_at, lease_expires);

CREATE TABLE outbox (
  delivery_id   BLOB PRIMARY KEY,
  record        BLOB NOT NULL,
  status        TEXT NOT NULL,
  run_at        INTEGER,
  lease_expires INTEGER
) STRICT;

CREATE INDEX outbox_poll ON outbox (status, run_at, lease_expires);

CREATE TABLE delivery_receipts (
  delivery_id BLOB PRIMARY KEY
) STRICT;

CREATE TABLE reactors (
  name         TEXT PRIMARY KEY,
  event_offset INTEGER NOT NULL
) STRICT;

CREATE TABLE projections (
  name         TEXT PRIMARY KEY,
  event_offset INTEGER NOT NULL,
  view         BLOB NOT NULL
) STRICT;

CREATE TABLE snapshots (
  aggregate_type TEXT    NOT NULL,
  aggregate_id   TEXT    NOT NULL,
  sequence       INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  history        TEXT    NOT NULL CHECK (history IN ('retained', 'compacted')),
  payload        BLOB    NOT NULL,
  PRIMARY KEY (aggregate_type, aggregate_id)
) STRICT;

CREATE TABLE schemas (
  kind           TEXT    NOT NULL CHECK (kind IN ('aggregate', 'projection')),
  name           TEXT    NOT NULL,
  schema_version INTEGER NOT NULL,
  PRIMARY KEY (kind, name)
) STRICT;
