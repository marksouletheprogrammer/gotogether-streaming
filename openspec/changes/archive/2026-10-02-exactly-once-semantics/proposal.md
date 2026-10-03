# Proposal

## Why

The current repository supplies a local Kafka foundation but no data-producing applications. Two comparable CNC cut pipelines will make Kafka transactional processing versus database-level deduplication observable without conflating their guarantees.

## What Changes

- Extend the default Docker Compose stack with a Java comparison application: two low-rate producers publish the same deterministic sequence of canonical `cut` records from `DATA.md` to separate cut-domain topics with distinct client IDs; encode Kafka values as Avro using a cut-only schema derived from the canonical JSON Schema.
- Add one consumer for the idempotent producer's source topic: it writes cuts directly to its PostgreSQL table and publishes a Kafka processed-cut output, committing that output and its source offset together in a Kafka transaction. PostgreSQL is outside the Kafka transaction and can receive duplicates on replay.
- Add one consumer for the at-least-once producer's source topic: it upserts cuts by `event_id` primary key in a separate PostgreSQL table, retaining one row per ID and replacing stored values when a later delivery with that ID differs. The two consumers use distinct groups; there is no third consumer or separate database sink.
- Register Avro value schemas for both cut sources and the processed-cut topic in the existing Schema Registry, without altering the base sample topic's JSON Schema subject.
- Add local PostgreSQL connectivity from Compose and host localhost, focused static/unit checks, one startup smoke check for service readiness, clean logs, and reachable endpoints, and concise run/inspection instructions. No assessment, event, forecasting, or changes to the canonical data contract are included.
- Extend the existing provisioned Grafana dashboard and metrics collection to show both cut producers and both consumer groups, including throughput and error rates for each producer and lag, throughput and error rates for each group; do not add a second dashboard or runtime mode.
- Start PostgreSQL, cut topics, two producers, two consumers, and their metrics with ordinary `docker compose up`, alongside the existing broker, registry, AKHQ, Prometheus, and Grafana. Keep the canonical `cnc-demo-records` topic empty and its JSON Schema unchanged; new records use separate cut topics. **BREAKING**: the former base-only service-scope and no-application assertions no longer describe the default stack.

## Capabilities

### New Capabilities

- `exactly-once-semantics`: Two parallel CNC cut producer-consumer pipelines included in normal startup, Kafka transactional output/offset boundary for the first consumer, PostgreSQL persistence and replay behavior, producer and consumer-group monitoring, and verifiable local operation.

### Modified Capabilities

- `base-streaming-platform`: Replace the obsolete prohibition on application producers, consumers, and database services in the default stack; retain the empty canonical sample topic and existing JSON Schema, and extend its existing Grafana dashboard with cut-pipeline panels. This requires a matching amendment to the existing capability spec before implementation is treated as fully specified.

## Impact

Adds Java application/build/container scaffolding, cut-domain Kafka topics and Avro schema assets/subjects, a PostgreSQL service and separate tables, new scrape targets and panels in the existing Prometheus/Grafana stack, targeted unit/static tests, a startup smoke check, and operator instructions. Reuses the existing broker/network and `DATA.md` schema as the canonical source; does not rewrite `DATA.md`, repurpose `cnc-demo-records`, or alter its JSON Schema subject. Existing base-only smoke/config assertions and base-scope documentation need updating to verify the expanded default stack, while retaining checks for the existing infrastructure and empty sample topic.

Success means the two source topics and first consumer's processed topic contain decodable Schema Registry Avro values that preserve canonical cut semantics, schemas remain stable on restart, matched sequences and rates in both paths, one consumer per source topic writing directly to its own table, committed-only Kafka output and atomically committed output/offsets in the first consumer, one persisted row per `event_id` after upsert/replay in the idempotent path, with the latest delivered values stored, and live metrics for two producers and two consumer groups on the existing Grafana dashboard (including lag, throughput and error rates as applicable). Kafka transactions **do not** atomically commit PostgreSQL writes: duplicates in the first table after a crash/replay are permitted and must not be described as end-to-end exactly-once database writes.

Rollback is to stop the expanded stack, revert the new application and metrics configuration and restore the previous baseline configuration if desired, without removing the shared broker/registry or database volumes. Removing volumes or Kafka topics would destroy demo data and requires explicit approval; normal shutdown must preserve it.
