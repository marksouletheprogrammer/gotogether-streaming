# Design

## Context

See proposal.md for motivation and the two delta specs for behavior. `CutProducer` and `EventProducer` use bounded `*_RECORD_COUNT=30` loops followed by an idle sleep, while Compose sets both `*_INTERVAL_MS` values to 2,000. Cut values come from `CutRecordGenerator`; technician events come from `EventRecordGenerator`, which creates three installations followed by recurring inspections. `EventConsumer` already classifies a random failure before `applyIfUnprocessed`, with a confirmed DLQ send before offset commit. `scripts/init_cuts.sql` only creates tables if absent, so changing a CREATE TABLE statement alone will not update retained PostgreSQL volumes. The transactional table currently uses a surrogate `id` and `ingested_at` and accepts duplicate IDs; the other uses `event_id` as primary key and `updated_at`. `cuts-db-bootstrap` completes before consumers start. Tests use JUnit 5; `scripts/smoke_check.py` validates the local Compose stack.

## Goals / Non-Goals

**Goals:** Make normal startup produce active, still-matched demo data beyond the old 30-record window, converge existing and new cut tables to one replay-safe shape, and model a small blind spot between DLQ counts and reconciliation rows.

**Non-Goals:** No new record fields or changes to `DATA.md`, Avro subjects, Kafka topology, dashboard panel definitions, automatic recovery of silently lost events, or claims of atomic Kafka/PostgreSQL commits. This does not add a general outbox or enforce that independently restarted cut producers share the same absolute Kafka offset; the comparison is of records generated for corresponding sequence indices.

## Decisions

### 1. Continuous, index-driven publishing with shared pacing

Replace bounded loops and keep-alive sleeps with shutdown-aware paced loops in both producer roles. Use a single progression rule for the two cut producers, and keep the generated record a pure function of its sequence position rather than wall-clock process start; identical positions stay byte-equivalent even when producers begin at different times. Preserve synchronous acknowledged sends and existing metrics/error handling; do not skip failed sends simply to keep the clock. Change Compose interval defaults to 200 ms and remove `*_RECORD_COUNT` settings rather than interpreting a count of zero ambiguously. Ensure generators can advance without an `int` loop overflow: use a wide sequence counter, and for cuts start a new deterministic tool-instance lifecycle with `cut_index` reset to 1 before the canonical/Avro integer range is exhausted, preserving unique `event_id` and `DATA.md`'s per-tool cut ordering. Technician-event IDs remain unique for each new sequence index and installations precede inspections for the same tool. Shutdown hooks/interrupt handling stop cleanly; do not change the existing Kafka retry guarantees.

Alternative: set a very large finite count; it still stops and overflows at some point, failing the ongoing-production requirement. Wall-clock timestamps per producer were rejected because they break equality of matched cuts.

### 2. Converge cut tables and repository writes before resuming consumers

Use the current idempotent table's shape for **both** tables: `event_id TEXT PRIMARY KEY`, `payload JSONB NOT NULL`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`. Change `PostgresCutRepository` so both roles execute the same `INSERT ... ON CONFLICT (event_id) DO UPDATE` policy, replacing payload and updating `updated_at` for later delivery. Keep transactional consumer's ordering (database commit, then Kafka output/offset commit), abort behavior and read-committed Kafka observers. A replay may execute another PostgreSQL upsert or update its timestamp, but it cannot create another row; do not label this a distributed exactly-once transaction.

Implement an idempotent, transactional schema upgrade in `scripts/init_cuts.sql`, used by the existing `cuts-db-bootstrap`. On an existing transactional table, identify legacy columns and build the new shape; collapse duplicate `event_id` entries by latest `ingested_at`, breaking timestamp ties with greatest legacy `id`, and map that survivor's timestamp into `updated_at`. Leave the already shaped idempotent table's data intact. Guard reruns so they do not overwrite persisted rows or repeat destructive transformations. Back up the existing tables before running the upgrade on retained volumes, and fail the bootstrap rather than starting consumers against incompatible table definitions. Coordinate bootstrap so the old cut consumers cannot write during the migration; Compose dependency ordering only protects new consumers during startup, so the operator must stop the running stack before upgrading.

Alternative: make both tables duplicate-friendly with application-only deduplication; without a unique constraint, concurrent/replayed writes can still produce duplicate rows, contradicting the requested table invariant. Altering only CREATE TABLE statements was rejected because existing volumes would retain the old schema.

### 3. Explicit three-way simulated event outcome

Add `EVENT_SILENT_DROP_PROBABILITY` (default 0.05) alongside `EVENT_FAILURE_PROBABILITY` (default 0.2); validate finite values in [0, 1] and combined sum <= 1. For each unprocessed, validated event, take one random draw: DLQ for `[0, dlqProbability)`, silent loss for `[dlqProbability, dlqProbability + silentProbability)`, otherwise apply state normally. DLQ publishing remains acknowledged before committing; silent loss only commits the source offset without target/DLQ write or processed-marker change. Keep the existing reconciliation-before-publish path and avoid misclassifying database, invalid-input, or DLQ-publish failures as silent loss. Maintain consumer throughput semantics as records consumed; avoid emitting a processing-error indication for intentional silent loss, to preserve the demo's observability gap. Existing PostgreSQL unreconciled and Kafka DLQ metrics remain independent and unchanged; update README explanation and inspectable queries instead of adding dashboard panels.

Alternative: independent random draws for each failure category would make actual percentages ambiguous and could select both outcomes for one event. Dropping at the producer would lose the pre-publish reconciliation marker, so it would not demonstrate the intended metric blind spot.

## Risks / Trade-offs

- [Continuous streams grow Kafka topics, PostgreSQL tables and reconciliation rows] → Keep Kafka's existing 24-hour retention, choose a moderate 5 records/second per producer default, retain a positive interval override, and document storage monitoring and nondestructive shutdown; do not silently purge database history.
- [Existing duplicate transactional rows are consolidated during migration] → Require a backup first; choose the latest `(ingested_at, id)` deterministically and verify before/after counts and id uniqueness. Reverting schema alone cannot recover discarded copies.
- [Producer restart replays an indexed prefix] → Stable IDs and upserts/reconciliation preserve one row per ID, but duplicate Kafka records may appear across restarts and the two topics can be transiently out of step; document comparison by sequence identity rather than raw topic offset.
- [A replay of a changed event ID or cut ID may refresh `updated_at`] → Cut tables explicitly use later-delivery-wins; event target continues to protect `updated_at` for already processed IDs. The cut producer's deterministic generation prevents accidental changed values for an existing sequence index.
- [Silent loss is intentionally absent from DLQ and injected-failure indicators] → Document that an increasing unreconciled count with no corresponding DLQ growth can indicate simulated drops *or* failed sends; do not claim the counters alone prove a particular cause.

## Migration Plan

1. Before an apply run on an existing deployment, stop app services/Compose without removing named volumes and back up the two cut tables. Do not execute Docker commands without user permission.
2. Add focused tests for live sequences, three-way failure selection, schema upgrade and repository replay behavior before changing production code. Add idempotent upgrade logic and matching upserts, then continuous producers and consumer selection; update Compose and smoke checks.
3. Bring the stack up via its documented default Compose startup (with permission). `cuts-db-bootstrap` migrates/validates both tables before consumers resume; inspect uniqueness and identical definitions, producer activity beyond 30 records, matching generated cuts, DLQ/reconciliation divergence, logs and metrics. Re-run bootstrap against retained data to prove idempotence.
4. Roll back by stopping services, restoring the pre-migration cut-table backup and prior code/Compose configuration; disable silent loss first if needing immediate return to DLQ-or-success processing. Previously published Kafka messages remain retained according to topic policy and cannot be rolled back by the table restore.
