# Design

## Context

The stack (`compose.yaml`) already runs Kafka (KRaft, `broker:29092`, PLAINTEXT), Schema Registry, Postgres, three Java producers, three Java consumers (all from `local/mill-cuts-pipeline`), Prometheus, Grafana, and AKHQ. Prometheus scrapes the apps via the JMX exporter agent (`monitoring/cut-app-jmx.yml`, port 9406), which currently exposes only custom `mill_*` counters and, as such, no per-topic producer series.

StreamLens (pinned upstream commit `8b1cdd19ac120d9b0f81e80e7510ea05c13e4e64`, Apache-2.0) is a FastAPI + React app. Its `container/Dockerfile` builds both and serves on port 5000 with a `/health` endpoint. It reads clusters from a JSON file (path overridable via `CLUSTERS_JSON`) and keeps snapshots in memory. It discovers: topics and consumer groups via the Kafka admin API; schemas via Schema Registry; and producers via a fallback chain: Prometheus `sum by (client_id, topic)(kafka_producer_topic_metrics_record_send_total)` (only series with value > 0), then broker JMX, then offset changes. See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**
- StreamLens starts with `docker compose up` and shows all pipelines with named producers, topics, schemas, consumer groups with zero UI setup.
- Minimal footprint: config and compose only, no Java changes.

**Non-Goals:**
- No changes to dashboards, pipelines, topics, schemas, or AKHQ.
- No AI assistant configuration, no authentication, no Kafka Connect.

## Decisions

1. **Build from the upstream git repository at a pinned commit** using a Compose `build.context` git URL (`https://github.com/muralibasani/streamlens.git#<sha>`) with `dockerfile: container/Dockerfile`, tagged `local/streamlens:<short-sha>`. No published official image was identified. Alternatives: vendoring the source (large, licence/maintenance burden); building from `main` (non-reproducible). Matches the project's existing local-build pattern (`Dockerfile.jmx`).

2. **Cluster config as a mounted, committed file** `monitoring/streamlens/clusters.json`, mounted read-only and selected via `CLUSTERS_JSON`. Contents: `bootstrapServers: broker:29092`, `schemaRegistryUrl: http://schema-registry:8081`, `prometheusUrl: http://prometheus:9090`, `securityProtocol: PLAINTEXT`, `enableKafkaEventProduceFromUi: false`. Read-only is safe because snapshots are in memory; if the app attempts to write the file (e.g. UI edits), those edits simply fail rather than modifying the repo. Verify at apply time that startup works with a read-only mount; fall back to copying the file into a named volume if not. No `jmxHost`/`jmxPort` (Prometheus is preferred and the broker JMX port is not exposed).

3. **Producer detection via Prometheus, by exposing the standard Kafka client metric.** Kafka clients already register `kafka.producer:type=producer-topic-metrics,client-id=*,topic=*` MBeans (`record-send-total`). Add that object name to `includeObjectNames` in `cut-app-jmx.yml` plus one rule naming it `kafka_producer_topic_metrics_record_send_total` with `client_id` and `topic` labels (COUNTER). This yields exactly the series StreamLens queries, giving client-ID-level producer nodes. Because all app processes (including the event consumer's DLQ producer) use the same agent config, the DLQ writer also appears as a producer of the DLQ topic. Alternatives: broker-side `messagesinpersec` fallback (anonymous producers, topics only, and requires broker rules not present); relabeling the existing custom `mill_*_records_sent_total` counters (lacks a topic label); a custom exporter (new code).

4. **Dependencies and ordering:** `depends_on` broker, schema-registry (healthy), prometheus (started). Topic/schema bootstrap are not required for startup; the 60s refresh picks up new objects.

5. **Port/host binding:** `127.0.0.1:5000:5000`, `restart: unless-stopped` via `*service-defaults`, `mem_limit: 512m`, healthcheck on `/health` (curl is in the image). Port 5000 must be free; documented in README prerequisites.

6. **Tests** follow existing patterns: extend `tests/test_compose_contract.py` (service, port, build pin, mounts, healthcheck) and `tests/test_monitoring_contract.py` (JMX rule/ include, config file contents, dashboards untouched).

## Risks / Trade-offs

- [Upstream build needs internet and is slow on first start (npm + uv)] → Pin commit, document first-start expectation; image cached afterwards.
- [Compose git build contexts require a Compose/BuildKit version supporting them] → Document requirement; fallback is documenting a manual `git clone` context if unsupported.
- [Producer appears only once `record-send-total` > 0 and while the process runs] → Producer containers stay up after finite series; spec scenario covers this.
- [MBean name/attribute format may not match exporter pattern] → Verify against live `curl producer:9406/metrics` during apply and adjust the rule.
- [Topology snapshot refreshes every ~60s, so the graph lags startup] → Document; user can reload.
- [Read-only config mount may break startup] → Decision 2 fallback.

## Migration Plan

Add files, run `docker compose up -d` (existing containers are re-created only for apps whose mounted JMX config changed; restart of producer/consumer containers re-runs finite producers, which is expected demo behavior). Rollback per proposal.md.
