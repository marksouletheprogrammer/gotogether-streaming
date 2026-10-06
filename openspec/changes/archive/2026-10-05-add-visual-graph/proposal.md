# Proposal

## Why

The demo runs three Kafka pipelines (two CNC cut pipelines and a technician-event pipeline with a dead-letter topic), but today they can only be inspected as tables (AKHQ) and time series (Grafana). There is no picture of how producers, topics, schemas, and consumer groups connect end to end. A live topology graph makes the architecture understandable at a glance.

## What Changes

- Add StreamLens (https://github.com/muralibasani/streamlens, pinned to a commit) as an additional service in the default Docker Compose startup, reachable at `http://localhost:5000`.
- Pre-configure StreamLens with this project's cluster (broker, Schema Registry, Prometheus) so it appears with no manual setup in the UI.
- Make producers discoverable by StreamLens by exposing the Kafka producer per-topic send counter (labelled with client ID and topic) from the existing application JMX exporter to the existing Prometheus.
- Document the new service, address, and usage in the README.
- No change to AKHQ, Grafana dashboards, pipelines, topics, schemas, or data. **No breaking changes.**

## Capabilities

### New Capabilities
- `pipeline-visualization`: A StreamLens topology graph, started with the stack and pre-configured, that shows every pipeline's producers, topics, schemas, and consumer groups end to end.

### Modified Capabilities
<!-- None: existing requirements are unchanged; AKHQ and Grafana remain as-is. -->

## Impact

- `compose.yaml`: new `streamlens` service (built from a pinned upstream git commit), port `127.0.0.1:5000`.
- New `monitoring/streamlens/clusters.json` (cluster config).
- `monitoring/cut-app-jmx.yml`: additional include + rule for producer topic metrics (new Prometheus series only; existing series and dashboards unaffected).
- `README.md`, tests under `tests/` (compose/monitoring contract).
- First startup needs internet access to clone and build StreamLens (Node + Python image build).

## Rollback

Remove the `streamlens` service, `monitoring/streamlens/`, the added JMX rule and README section, then `docker compose up -d`. No data volumes or other services are touched. A running instance can be dropped with `docker compose rm -sf streamlens`.

## How to know it works

- `docker compose ps` shows `streamlens` healthy; `curl http://localhost:5000/health` returns success.
- Opening `http://localhost:5000` lists the pre-configured cluster; its topology shows producers `mill-cuts-transactional-producer`, `mill-cuts-at-least-once-producer`, `mill-tool-events-producer` linked to their topics, and consumer groups `mill-cuts-transactional-consumer`, `mill-cuts-idempotent-consumer`, `mill-tool-events-consumer`, plus schema nodes and the committed and DLQ topics.
- Prometheus query `kafka_producer_topic_metrics_record_send_total` returns series with `client_id` and `topic` labels.
- AKHQ (8080) and both Grafana dashboards (3000) behave as before.
