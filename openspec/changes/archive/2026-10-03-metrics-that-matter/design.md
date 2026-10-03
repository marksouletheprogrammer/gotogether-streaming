# Design

## Context

See `proposal.md` for motivation and `specs/metrics-that-matter/spec.md` for observable behavior. The current Compose stack already provides Kafka, Schema Registry, PostgreSQL, Prometheus, Grafana file provisioning, a Java 21 Maven image, schema bootstrap jobs, cut-specific JMX counters, and a Kafka group-lag exporter. Its `cut` paths and the empty `cnc-demo-records` sample must keep working. `DATA.md` is the sole source of synthetic-data semantics: `event` is a distinct technician-history record kind, not a new sensor schema.

## Goals / Non-Goals

**Goals:** Make the Kafka DLQ topic length, unprocessed event rows, and per-tool freshness independently observable; keep one ordinary Compose startup and one monitoring stack; make event processing replay-safe in the database.

**Non-Goals:** Distributed atomicity across Kafka and PostgreSQL; exactly-once Kafka DLQ publication; an outbox or automatic recovery of failed producer sends; a DLQ replay/repair UI; diagnosis or forecasting from technician events; new fields or edits to `DATA.md`; changes to cut pipeline guarantees.

## Decisions

### 1. Model technician events as their own canonical Avro variant

Use `mill-tool-events-source` and `mill-tool-events-dlq`, with a distinct `mill-tool-events-consumer` group and `mill-tool-events-producer` client. Generate a finite low-rate series for multiple physical `tool_instance_id`s (for example `cutter-1` to `cutter-3`), starting with `tool_installed` events and later revisiting each tool with `inspection` events using `component: cutter` and `confirmed_fault: none` or `unknown`. Do not invent readings, cut history, or fault labels; omit optional `cut_index` when not known. Derive an `event`-specific Avro record from the `DATA.md` JSON Schema variant, mapping optional payload fields to nullable Avro unions and validating all JSON Schema semantics (required fields, enums, conditional inspection/maintenance fields, IDs, constants and RFC 3339 timestamps) before sending or persisting. Use stable sequence-based event IDs and a fixed simulated start time (as the cut generator does) so a restart regenerates the same record. Register/verify both event `<topic>-value` subjects in the same non-destructive, conflict-rejecting style as `register_cut_schemas.py`; keep existing cut and sample subjects untouched.

*Alternative:* Reuse the cut Avro schema or send JSON into the sample topic. Rejected: it changes the entity being demonstrated and violates the new Avro topic boundary.

### 2. Commit a simple reconciliation row before each send

Create `mill_tool_event_reconciliation` keyed by `event_id` with `processed BOOLEAN NOT NULL DEFAULT FALSE` and a creation timestamp. For each generated event, insert the row and commit the PostgreSQL transaction before Kafka `send`; use an idempotent insert on restart so an existing row is not duplicated or changed from processed back to unprocessed. Wait for the send acknowledgment and report any failure. If the DB commit fails, do not send. If the send fails afterward, leave the row unprocessed; no publisher outbox, publication status, payload copy, or background retry of unsent rows is required. A subsequent manual restart may emit the same deterministic event again; the consumer's processed flag handles such redelivery. The row count answers whether produced IDs were processed, not whether every ID reached Kafka.

*Alternative:* Send first and insert a row after Kafka acknowledgment. Rejected: the consumer can race the insert, and a crash between send and insert leaves an event with no reconciliation row. The chosen DB-first ordering intentionally permits an unprocessed row when publication fails; this is acceptable for the demo and must not be described as an atomic Kafka/Postgres publish.

### 3. Upsert one current state per entity and handle replays without refreshing time

Create `mill_tool_event_state` keyed by `(machine_id, tool_instance_id)` with latest `event_id`, canonical event payload, and non-null `updated_at`. Use one partition with Kafka key derived from that composite entity to preserve per-tool order at this demo's scale. For each valid source event, lock/check its reconciliation row; when `processed = true`, commit the source offset without changing the target. Otherwise upsert the current-state row and set the reconciliation row `processed = true` in one PostgreSQL transaction, with `updated_at` set to the database processing time only for a new successful event. Commit the Kafka offset after DB commit. Unexpected DB errors must abort the DB transaction and leave the offset retryable rather than being treated as simulated failures. Per-event timestamps represent arrival-to-target updates, not the source `occurred_at` time.

*Alternative:* Append every event to the target or advance a row on every retry. Rejected: those approaches count repeated entities more than once in staleness or make replay appear to improve freshness.

### 4. Dead-letter only deliberate simulated processing failures

After schema validation and checking whether an event is already processed, sample a configurable probability (`0` for deterministic success tests, `1` for deterministic failure tests; positive small default for the demo). For a selected event, publish its original Schema Registry Avro value to `mill-tool-events-dlq` keyed by `event_id`, with a UTF-8 `error` header describing the simulated failure (and optional source location headers). After Kafka acknowledgment, commit the source offset without updating the reconciliation row or target table. If DLQ publication fails, leave the source offset uncommitted and retry. A crash after DLQ acknowledgment but before source-offset commit may produce another retained DLQ record on replay; the row stays unprocessed. The DLQ is not consumed automatically and its Kafka record count does not determine the reconciliation flag. Do not expose database exception details or secrets in headers.

*Alternative:* Persist a `dead_letter` database status and treat it as queue depth. Rejected: a Kafka record may be duplicated or expired independently of a database row, and the requested DLQ metric measures the Kafka topic itself.

### 5. Measure independent Kafka and database outcomes

Add event producer/consumer JMX counters (sent, send errors, processed, processing failures) and extend the existing lag exporter with the event consumer group without changing the cut series. In that Kafka-connected exporter, query beginning and latest offsets for each `mill-tool-events-dlq` partition and sum `latest - earliest` as `mill_tool_events_dlq_topic_length`. Expose a separate DLQ collection-success signal, independent of the existing cut/event group-lag collection: missing Kafka offsets must not be reported as zero, and a DLQ query failure must not hide otherwise healthy lag samples. Retained duplicate records count individually and the value can decrease as Kafka retention removes records.

A separate always-on Java HTTP exporter, built into the existing app image and running independently of the finite producer, queries PostgreSQL and exposes `mill_tool_events_unreconciled` as `COUNT(*) WHERE processed = false` and `mill_tool_events_average_staleness_seconds` as the mean of database `now - updated_at` across current-state rows. A zero-row target omits the staleness gauge. Publish `mill_tool_events_outcome_collection_success` to distinguish failed database queries from healthy zero counts, and do not serve stale successful gauges on a failed refresh. Prometheus scrapes the event processes, lag/DLQ exporter and database exporter, displaying target `up` and the separate collection statuses where relevant.

*Alternative:* Derive DLQ depth from reconciliation rows or calculate freshness from source `occurred_at` / a per-process counter. Rejected: a database processed flag has no knowledge of Kafka topic length, source event time is not when the target was updated, and in-process counters are lost on restart.

### 6. Clone the updated conventional dashboard and protect equality with a test

Extend `base-streaming-platform.json` with conventional event panels while leaving existing panels intact. Add `metrics-that-matter.json` with the same panels and targets, a distinct `uid`/title, and three appended outcome panels. The existing Grafana provider already loads all JSON files in the dashboards directory; add a contract test comparing the normalized shared panels and verifying that DLQ length queries the Kafka-exported series while unreconciled count and staleness query the database-exported series. Extend `tests/test_monitoring_contract.py`, `tests/test_compose_contract.py`, Java tests, and `scripts/smoke_check.py` rather than replacing existing cut assertions; the compose contract's hardcoded total of two producers/two consumers should be narrowed to cut-specific service counts. Update README with concise inspect/start/stop steps and explicitly distinguish the Kafka and database metrics.

*Alternative:* Replace the original dashboard with the new panels. Rejected: that would eliminate the intended side-by-side comparison.

## Risks / Trade-offs

- [Postgres row commit and Kafka publish are not atomic] → Never publish without a committed row; surface send failures, leave failed sends unprocessed, and document that automatic resend is out of scope.
- [A replay can add duplicate Kafka source or DLQ records] → Use stable IDs and the processed flag to avoid duplicate target updates; measure retained DLQ copies as Kafka records, not unique IDs.
- [DLQ retention changes topic length independently of reconciliation] → Document that DLQ length is retained records, not the number of unresolved event IDs.
- [A dead-lettered event has no automatic repair path] → Inspect its Kafka error header; its reconciliation row remains unprocessed. Automated replay can be designed separately if requested.
- [Finite source runs and probability-based injection can produce no failures in a particular run] → Keep JMX endpoints alive after completion, use configurable `0`/`1` in focused tests, and do not make smoke checks rely on random DLQ occurrence.
- [Kafka or database metrics polling fails] → Expose independent collection-health signals and suppress stale gauge samples on failed refresh; use no-data for empty staleness rather than `0`.
- [Existing static tests assume exactly two total producers/consumers] → Adjust those assertions to apply to cut services only, while preserving their cut requirements.

## Migration Plan

1. Add event schema generation/verification and additive SQL/topic bootstrap. Ensure topic, subject, and table creation is idempotent and retains existing state.
2. Add the event producer/consumer and health-gated Compose services; preserve existing cut startup dependencies. Add Kafka DLQ topic-length collection, independent database outcome metrics, scrape targets, and dashboard JSON files.
3. Run unit and static contract checks, including DB-before-send/consumer replay boundaries, Kafka topic-offset counts, database counts, and dashboard comparison. With the user's permission for Docker commands, run the normal Compose startup and smoke check, inspect PostgreSQL/topic/Schema Registry results, and re-run startup without removing volumes.
4. To roll back, disable only the new event services and scrape targets and remove the added dashboard/panels via a follow-up configuration change. Retain Kafka topics and PostgreSQL tables for diagnosis; do not run volume deletion or alter cut resources.
