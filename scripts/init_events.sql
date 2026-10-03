CREATE TABLE IF NOT EXISTS mill_tool_event_reconciliation (
    event_id TEXT PRIMARY KEY,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mill_tool_event_state (
    machine_id TEXT NOT NULL,
    tool_instance_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (machine_id, tool_instance_id)
);
