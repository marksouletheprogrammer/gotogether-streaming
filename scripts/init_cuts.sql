CREATE TABLE IF NOT EXISTS mill_cuts_transactional_writes (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mill_cuts_idempotent_writes (
    event_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
