# Proposal

## Why

The demo's three producers stop after 30 messages, leaving throughput and outcome dashboards idle. The cut tables also expose incompatible shapes, and the event pipeline demonstrates only visible DLQ failures rather than undetected loss.

## What Changes

- Replace finite cut and technician-event runs with continuously paced publishing, at a substantially higher default rate (proposed 200 ms per record instead of 2,000 ms), until orderly shutdown. Retain configurable pacing, canonical `DATA.md` fields and Avro values; both cut source topics must carry the same indexed record sequence.
- **BREAKING**: Make `mill_cuts_transactional_writes` and `mill_cuts_idempotent_writes` identical in columns, types, defaults, primary/unique constraints, and update behavior: one row per `event_id` in each. Both consumers deduplicate/upsert before committing their Kafka offsets; the transactional path still commits its Kafka output and offset together, without claiming a distributed Kafka/PostgreSQL transaction. Migrate existing retained rows without resetting volumes; reconcile pre-existing duplicate transactional IDs deterministically.
- Add a separate, configurable simulated silent-drop outcome for a small percentage of technician events (proposed 5% of all eligible events), alongside the existing 20% DLQ-failure path. Dropped events advance the source offset but produce no target update or DLQ entry and leave the reconciliation row unprocessed. Actual infrastructure errors must remain visible and retryable.
- Update focused tests, startup checks, and concise operating instructions for ongoing streams, replay-safe tables, and distinguishing silent losses using reconciliation versus DLQ counts.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `exactly-once-semantics`: Replace finite low-volume matched cuts and duplicate-permitting transactional database behavior with continuously matched, higher-volume cuts and identical replay-safe tables.
- `metrics-that-matter`: Replace finite low-volume technician events with continuous generation; add simulated silent loss distinct from DLQ failures and reflect its effects in reconciliation metrics.

## Impact

Cut and event producer loops/generators, event consumer outcome handling and metrics, cut PostgreSQL initialization/migration and repository writes, Compose defaults, Java tests, smoke checks, and README. No new external services, record fields, or dependencies are proposed. Existing `exactly-once-semantics` requirements explicitly permitting a duplicate transactional database row and describing finite production will be superseded; Kafka/PostgreSQL atomicity remains out of scope.

## Rollback

Stop the Compose services without deleting volumes. Back up both cut tables before migration; restoring the backup and previous app/Compose configuration reverses the table change (a schema-only rollback cannot restore any older duplicate rows merged by migration). Reverting to finite rates stops new publishing but retains the messages already emitted. Disable the silent-drop setting (zero probability) to restore DLQ-or-success behavior.

## Verification

Unit and migration tests should show matching cut records for equal indices, continuously advancing counts over multiple former 30-record windows, valid ongoing event records, matching cut table definitions and no duplicate `event_id` after replay or migration, and three disjoint event outcomes. On the local default Compose stack, verify producer throughput remains nonzero, reconciliation grows from silently dropped records even when no matching DLQ entry appears, and both cut tables remain unique by `event_id`; perform runtime Docker verification only with user permission.
