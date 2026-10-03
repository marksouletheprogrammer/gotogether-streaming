# Design

## Context

See `proposal.md` for motivation and `specs/base-streaming-platform/spec.md` for behavior. This is a greenfield repository: `PROMPT.md`, `DATA.md`, and OpenSpec configuration exist, but there is no Compose stack, source tree, test harness, or existing capability spec. `DATA.md` contains a complete Draft 2020-12 schema for `cut`, `event`, and `assessment` records and three illustrative records. No Java or Postgres runtime is needed until a later data-processing feature.

## Goals / Non-Goals

**Goals:**
- One reproducible Compose entry point with an internal service network, host-only access to operator endpoints, dependency readiness checks, and disposable Kafka/registry/metrics state.
- A single canonical contract, deterministically extracted from `DATA.md`, usable by the registry and checked for drift.
- A dashboard whose queries correspond to exported broker and registry metrics, with verification against live endpoints rather than merely checking that a JSON dashboard exists.

**Non-Goals:**
- Production availability, TLS, auth, remote access, broker scaling, live telemetry, predictions, or application backend/database infrastructure.
- Treating the schema registry as a validator of all cross-record rules; these require future application logic.

## Decisions

### Lightweight paired Kafka and registry images

Use matching, pinned `confluentinc/cp-kafka` and `confluentinc/cp-schema-registry` 8.1.x images (for example 8.1.1), with a single combined broker/controller KRaft node, one replica for internal topics, and a fixed cluster ID for a local demo. Keep Kafka logs (including the registry's Kafka-backed schema state) and monitoring data in Compose-managed volumes while running; the documented teardown uses `docker compose down --volumes` to remove them for the next clean startup. This paired distribution provides a compatible registry for the Draft 2020-12 contract and a known Compose configuration. Pin AKHQ, Prometheus, and Grafana to vetted versions too; do not float `latest`. Compared with separate Apache Kafka/registry vendors, a matched release reduces compatibility risk; compared with multi-node Kafka, it keeps a local demo manageable. Confirm chosen tags and registry schema support when building; do not silently downgrade the schema dialect.

Use named listeners: internal `broker:29092`, host `localhost:9092` bound to 127.0.0.1, and an internal-only controller listener. Advertise addresses appropriate to each listener, rather than advertising localhost to other containers. Publish AKHQ on `127.0.0.1:8080`, schema registry on `127.0.0.1:8081`, Grafana on `127.0.0.1:3000`, and Prometheus on `127.0.0.1:9090` for local health checks; JMX exporters remain network-internal. Enable Grafana anonymous Viewer and no AKHQ login only because host publishing is loopback-only. Configure service health checks and Compose dependencies so bootstrap waits for broker and registry readiness; a nonzero bootstrap exit must remain visible rather than being ignored.

### Fresh-start sample setup and safe repeats

Use short-lived, health-gated Compose `topic-bootstrap` and `schema-bootstrap` jobs, not a long-running producer/consumer. On fresh state, create `cnc-demo-records` with one partition, replication factor one, and low-footprint retention, then register the complete schema under `cnc-demo-records-value` via registry REST as JSON Schema. Extract the first JSON Schema fenced block from `DATA.md` into a checked-in generated schema asset (e.g. `schemas/cnc-demo-records.schema.json`) with a deterministic extractor that errors if the schema block is absent/ambiguous. A static test compares parsed asset content to the canonical block and validates its three illustrative records plus negative records using a pinned Draft 2020-12 validator with format checking. This preserves the single source of semantics while giving schema setup an ordinary mounted JSON file. On repeated `docker compose up`, topic creation skips an existing topic, and schema setup retrieves the latest subject: it registers only if absent, reuses a structurally identical JSON Schema without creating another version, and exits nonzero rather than modifying a conflicting subject. Existing topic data and monitoring history are preserved; the smoke check's zero-offset assertion applies to fresh demo state. No sample record is sent.

### Instrumentation and dashboard

Export JMX from both Kafka and Schema Registry through pinned Prometheus JMX exporter javaagents added to small derived container images, using a verified release artifact/checksum and narrow metric rules. Prometheus scrapes each service's internal exporter port and stores data in a Compose-managed volume that is deleted on teardown; Grafana provisions its Prometheus datasource and a checked-in dashboard automatically. Include `up` targets, Kafka broker/controller health, bytes-in/out and request count/latency or errors, and registry request count/errors/availability; validate metric names against actual `/metrics` output and adjust rules and panels together rather than relying on a third-party dashboard that assumes different labels. Idle traffic legitimately yields zero throughput; an absent series or down target is not reported as zero. Registry REST requests by bootstrap and smoke checks provide real request counters. Prometheus may expose localhost API for checking target status; only loopback host ports are published.

### Verification and documentation

Keep tooling small: a static contract/config test (including `docker compose config`, schema drift and positive/negative JSON Schema checks) and a read-only smoke test after `docker compose up -d` that checks health, host/container metadata reachability, empty topic offsets, registry subject content, AKHQ/Grafana responses and provisioned dashboard, and nonempty Prometheus series/up for broker and registry. Put test prerequisites and exact commands in a concise `README.md` with startup, verification, port map, and explicit data-discarding teardown (`docker compose down --volumes`). No Docker commands, including `docker compose config`, are to be run by the implementation agent without user permission, per project apply guidance; schema-only static checks can run beforehand.

## Risks / Trade-offs

- [Single node is not fault tolerant] → Label this explicitly as a local demo and keep replication factor one.
- [Confluent and monitoring images can be memory intensive] → Limit heap/JVM settings, keep only required services, and document modest Docker memory needs rather than adding unused components.
- [Schema compatibility or JMX names differ by release] → Use matched pinned versions and validate real registry registration and scraped metric names end-to-end before accepting the stack.
- [Unauthenticated endpoints are unsafe on a public interface] → Publish only to 127.0.0.1, avoid external access and document the local-only constraint.
- [Generated schema can diverge from `DATA.md`] → Fail static contract checks on drift before deployment; only change the canonical document with explicit user authorization.
- [Teardown permanently discards demo data] → Document `docker compose down --volumes` prominently; do not store irreplaceable records in this disposable stack.
- [Initial image downloads and dashboard scrapes take time] → Health-gate bootstrap and use bounded retries/timeouts in smoke checks, reporting actual failures clearly.

## Migration Plan

There is no existing deployment to migrate. Add and validate the files, start with `docker compose up -d`, confirm the setup job succeeded and run the smoke checks. Teardown/rollback uses `docker compose down --volumes` to remove this project's Compose-managed data, then reverts newly added files if needed; a later startup creates a new empty topic, schema registration, and metrics history. Executing this destructive teardown requires user approval for that specific command.
