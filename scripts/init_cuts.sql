-- Migration: Converge both cut tables to identical schema with event_id as primary key
-- This script is idempotent and handles both fresh databases and migrations from legacy schema

-- Step 1: Create a temporary table to hold the new schema for the transactional table
-- (only if the old transactional table exists with the legacy schema)
DO $$
BEGIN
    -- Check if the old transactional table exists with the legacy schema (has 'id' column)
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'mill_cuts_transactional_writes'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'mill_cuts_transactional_writes' AND column_name = 'id'
    ) THEN
        -- Legacy transactional table exists; migrate it
        -- Create temp table with new schema
        CREATE TEMP TABLE mill_cuts_transactional_writes_new AS
        SELECT DISTINCT ON (event_id)
            event_id,
            payload,
            ingested_at AS updated_at
        FROM mill_cuts_transactional_writes
        ORDER BY event_id, ingested_at DESC, id DESC;
        
        -- Drop old table and rename new one
        DROP TABLE mill_cuts_transactional_writes;
        CREATE TABLE mill_cuts_transactional_writes (
            event_id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        INSERT INTO mill_cuts_transactional_writes SELECT * FROM mill_cuts_transactional_writes_new;
    ELSE
        -- Fresh database or already migrated; create the new schema directly
        CREATE TABLE IF NOT EXISTS mill_cuts_transactional_writes (
            event_id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    END IF;
END $$;

-- Step 2: Create the idempotent table with the same schema (idempotent)
CREATE TABLE IF NOT EXISTS mill_cuts_idempotent_writes (
    event_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
