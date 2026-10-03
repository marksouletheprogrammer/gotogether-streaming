# Proposal

## Why

The current two cut pipelines show conventional throughput, error, and lag metrics, but they do not show whether source records were actually applied or how stale each entity's latest stored state is. A separate technician-event pipeline makes these outcomes visible alongside ordinary Kafka monitoring without changing the existing cut comparison.

## What Changes

- Add a low-rate `event` pipeline (a different `DATA.md` record kind from `cut`) with its own producer, Avro source topic, consumer group, PostgreSQL reconciliation and target tables, and Avro dead-letter topic.
- Commit one reconciliation row per event before publishing it. On successful consumption, update the target and mark its reconciliation row processed in one database transaction. Introduce configurable random processing failures that send the original event to the DLQ with error details in Kafka headers, without using reconciliation rows to track DLQ membership.
- Keep the provisioned base dashboard's broker, registry, and cut panels; add conventional throughput, errors, and lag monitoring for the event pipeline. Provision a second dashboard with the same baseline panels plus Kafka DLQ topic length, the number of unprocessed reconciliation rows, and average entity staleness.
- Start the new services with the ordinary Compose stack, extend focused tests and the startup smoke check, and document how to inspect the new topics, tables, and dashboards. Leave `DATA.md`, the canonical sample topic/schema, and both cut pipeline behaviors unchanged.

## Capabilities

### New Capabilities

- `metrics-that-matter`: Technician-event ingestion, reconciliation, dead-letter handling, and outcome-focused monitoring.

### Modified Capabilities

None. The existing `base-streaming-platform` and `exactly-once-semantics` requirements remain unchanged; the added event services and dashboards are additive.

## Impact

- New event-domain Java generation, validation, producer/consumer, and metrics components within the existing Maven application; event-derived Avro schema and bootstrap/registration support.
- Additive Compose topic/database/service setup, Prometheus scrape targets and event metrics, a second provisioned Grafana dashboard, focused Java/Python tests, smoke checks, and README instructions. Existing cut tables, Kafka subjects, and Grafana dashboard remain usable.
- Success: default startup produces valid Avro `event` records with reconciliation rows committed before send; successfully consumed IDs are marked processed with corresponding current target rows; configured injected failures appear in the Kafka DLQ with error headers and remain unprocessed. The second dashboard obtains DLQ length from the Kafka topic, unreconciled count from unprocessed database rows, and staleness from target updates. Repeated startup does not erase stored data or create redundant schema versions.
- Rollback: stop/disable only the new event producer, consumer, and event-specific monitoring/exporter services, and remove the new dashboard and scrape configuration in a follow-up change. Retain the new topics and PostgreSQL tables for inspection rather than deleting persisted data; the original cut pipelines and dashboard continue to operate.
