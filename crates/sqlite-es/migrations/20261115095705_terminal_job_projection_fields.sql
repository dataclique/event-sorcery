-- Terminal states carry their kind in an object payload. Virtual generated
-- columns allow replacing the expressions without recreating caller sidecars.
-- The polling indexes still store the computed lookup keys.
DROP INDEX job_queue_runnable;
DROP INDEX job_queue_reclaim;

ALTER TABLE job_queue_projection DROP COLUMN kind;
ALTER TABLE job_queue_projection DROP COLUMN status;

ALTER TABLE job_queue_projection ADD COLUMN kind TEXT GENERATED ALWAYS AS (
    COALESCE(
        json_extract(payload, '$.Live.Pending.kind'),
        json_extract(payload, '$.Live.Claimed.kind'),
        json_extract(payload, '$.Live.Done.kind'),
        json_extract(payload, '$.Live.Dead.kind')
    )
) VIRTUAL;

ALTER TABLE job_queue_projection ADD COLUMN status TEXT GENERATED ALWAYS AS (
    CASE
        WHEN json_type(payload, '$.Live.Pending') IS NOT NULL THEN 'pending'
        WHEN json_type(payload, '$.Live.Claimed') IS NOT NULL THEN 'claimed'
        WHEN json_type(payload, '$.Live.Done') IS NOT NULL THEN 'done'
        WHEN json_extract(payload, '$.Live') = 'Done' THEN 'done'
        WHEN json_type(payload, '$.Live.Dead') IS NOT NULL THEN 'dead'
        ELSE NULL
    END
) VIRTUAL;

CREATE INDEX job_queue_runnable ON job_queue_projection (kind, status, run_at);
CREATE INDEX job_queue_reclaim ON job_queue_projection (kind, status, lease_until);
