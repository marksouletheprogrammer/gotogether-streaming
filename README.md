# GoTogether Streaming Platform

A local Docker Compose streaming demo for CNC cut data and technician events. The default stack starts Kafka in single-node KRaft mode, Schema Registry, PostgreSQL, three producers, three consumers, a lag/DLQ exporter, an event outcome exporter, AKHQ, Prometheus, and Grafana. The two cut producers each continuously publish the same unbounded Avro series at a 200-millisecond interval until shutdown; the event producer continuously publishes its own event series at the same rate. Both cut tables use identical schemas with `event_id` as the primary key, enforcing one row per ID. The event consumer supports configurable simulated DLQ failures (20% default) and silent drops (5% default) to demonstrate observability gaps. Producer and exporter processes remain available for metric scraping. The canonical sample topic remains empty.

## Requirements

- Docker Engine or Docker Desktop with the Compose plugin; first startup needs internet access to pull pinned images and build the checksum-verified JMX exporter.
- Python 3.13 or newer for the contract and smoke checks.
- About 4 GiB of memory available to Docker and free localhost ports 5432, 8080, 8081, 9090, 9092, 3000, and 5000.
- On first startup, Docker needs internet access and git-build-context support (a recent Compose with BuildKit) to clone and build StreamLens from its pinned upstream commit; the image is cached afterwards.

## Static checks

From the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-test.txt
python3 scripts/extract_schema.py
python3 scripts/derive_cut_avro_schema.py --check
python3 scripts/derive_event_avro_schema.py --check
python3 scripts/derive_metrics_that_matter_dashboard.py --check
python -m unittest discover -s tests -v
```

`DATA.md` is the authoritative synthetic-data contract. The checked-in cut and event Avro schemas are derived from its record variants; the base `cnc-demo-records` topic continues to use its existing JSON Schema.

## Start and verify

```sh
source .venv/bin/activate
docker compose config
docker compose up -d --build
docker compose ps -a
python scripts/smoke_check.py
```

The smoke check verifies service state and startup logs, Kafka metadata and topics, Schema Registry subjects, the empty canonical sample topic, event PostgreSQL tables, Prometheus targets and live metrics, and both provisioned Grafana dashboards. Setup jobs should exit with status 0; long-running services should be running, with Kafka, Schema Registry, and PostgreSQL healthy.

Re-running `docker compose up -d --build` preserves existing volumes. Topic, schema, and table setup is repeatable and refuses conflicting schemas rather than replacing them. The cut and event producers use separate topics, so neither changes the empty `cnc-demo-records` topic.

## Key Features

### Continuous, matched cut producers
Both cut producers run indefinitely at a configurable pacing interval (200 ms default), generating the same deterministic sequence of cuts indexed by completion. Each producer has its own source topic and consumer group, but both emit identical records for the same sequence index. Producers stop cleanly on shutdown without partial records.

### Identical, replay-safe cut tables
Both `mill_cuts_transactional_writes` and `mill_cuts_idempotent_writes` tables use the same schema: `event_id TEXT PRIMARY KEY`, `payload JSONB NOT NULL`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`. Both consumers upsert by `event_id`, guaranteeing one row per ID. The transactional consumer commits its database write before its Kafka transaction; the idempotent consumer commits its source offset only after the database write succeeds. Replays do not create duplicate rows.

On first startup with retained volumes, the migration in `scripts/init_cuts.sql` converges the legacy transactional table schema to the new shape, collapsing duplicate `event_id` entries by latest `(ingested_at, id)` deterministically. The migration is idempotent: reruns do not corrupt data or repeat destructive transformations. **Back up both cut tables before upgrading an existing deployment.**

### Simulated silent loss alongside DLQ failures
The event consumer supports two independent, mutually exclusive simulated failure modes:
- **DLQ failure** (20% default): publishes the original event to `mill-tool-events-dlq` with an error header, leaves the reconciliation row unprocessed, and commits the source offset.
- **Silent drop** (5% default): commits the source offset without writing to the target, DLQ, or reconciliation row, leaving the event unprocessed and invisible to DLQ metrics.

Both probabilities are configurable and must sum to ≤ 1. Set both to 0 for deterministic success. Silent drops demonstrate the observability gap between unreconciled event counts (which include them) and DLQ topic length (which does not).

## Pipelines

```text
Transactional cut producer -> mill-cuts-transactional-source -> transactional cut consumer -> mill_cuts_transactional_writes
                                                                    +-> mill-cuts-committed
At-least-once cut producer -> mill-cuts-atleastonce-source -> idempotent cut consumer -> mill_cuts_idempotent_writes
Technician-event producer -> mill-tool-events-source -> mill-tool-events-consumer -> mill_tool_event_state
                                                                                     +-> mill-tool-events-dlq
```

The cut producer client IDs are `mill-cuts-transactional-producer` and `mill-cuts-at-least-once-producer`; their consumer groups are `mill-cuts-transactional-consumer` and `mill-cuts-idempotent-consumer`.

The technician-event producer is `mill-tool-events-producer`; its consumer group is `mill-tool-events-consumer`. It emits canonical `event` records from `DATA.md` for multiple tool instances, including repeated inspections for the same tool. Both event Kafka topics use Schema Registry-framed Avro values:

- `mill-tool-events-source-value`
- `mill-tool-events-dlq-value`

The event producer commits one row to `mill_tool_event_reconciliation` before sending each event. If that database commit fails, it does not send. A Kafka send failure leaves the row with `processed = false`; there is no automatic publisher outbox or background resend. A later manual restart can send the same deterministic event again. The consumer uses the row's `processed` flag to skip reapplying a successfully handled event.

On successful processing, the consumer updates the latest state for a `(machine_id, tool_instance_id)` and marks that event's reconciliation row processed in one PostgreSQL transaction. Kafka offset commits happen after that database transaction; Kafka and PostgreSQL do not share a distributed transaction. The target table's `updated_at` records the time the target was successfully updated.

`EVENT_FAILURE_PROBABILITY` defaults to `0.2` and can be set to `0` for deterministic success checks or `1` for deterministic DLQ checks. An injected DLQ failure publishes the original Avro event to `mill-tool-events-dlq` with an `error` header before committing the source offset; it does not update the target or mark the reconciliation row processed. Inspect the DLQ topic and its error header in AKHQ. DLQ messages are not automatically replayed.

`EVENT_SILENT_DROP_PROBABILITY` defaults to `0.05` (5%) and can be set to `0` to disable silent drops. An injected silent drop commits the source offset without writing to the target, DLQ, or reconciliation row. The event remains unprocessed and invisible to DLQ metrics, but the unreconciled count includes it. This demonstrates the observability gap between reconciliation metrics (which count all unprocessed events) and DLQ metrics (which count only failed sends). Both probabilities must sum to ≤ 1.

### Metrics that matter

Grafana provisions two dashboards at `http://localhost:3000`:

- **CNC Streaming Operations** shows broker, Schema Registry, cut, and conventional event producer/consumer throughput, lag, and component health. Consumer processing errors are not shown on this operational view.
- **CNC Event Outcomes (Improved)** contains all operational panels from CNC Streaming Operations plus the consumer processing-error time series and three outcome metrics below, organized in a dedicated **Event outcomes** row.

Both dashboards share three selectors: **Producer client ID**, **Consumer group ID**, and **Topic**; each defaults to All available values. **Kafka throughput** and **Producer throughput** intentionally show the same producer records-per-second series by client ID, not broker byte rates; both and **Producer send errors** follow the producer selector. Consumer throughput and lag panels follow the consumer-group selector; consumer processing errors appear only on CNC Event Outcomes (Improved) and also follow the consumer-group selector. Topic filters only the topic-labeled lag panel.

The CNC Streaming Operations dashboard is organized into **Broker and registry**, **Producer activity**, **Consumer activity and lag**, and **Component health** sections. Each health panel reports **Healthy (green)** for a reachable target, **Unhealthy (red)** for a failed scrape, and **Unavailable (gray)** when health telemetry is missing.

CNC Event Outcomes (Improved) adds a dedicated **Event outcomes** row containing exactly three outcome panels before the Component health section: **Dead-letter topic length** and **Unreconciled event rows** are stat panels, while **Average entity staleness** is a time series measured in seconds. Each outcome panel contains only its own metric value; collection-health series are not embedded in outcome panels. A healthy zero count is distinct from missing or unavailable source telemetry.

These metrics have separate sources and meanings:

- `mill_tool_events_dlq_topic_length` is the retained record count from the Kafka DLQ topic, summed as latest offset minus earliest retained offset across its partitions. Duplicate DLQ records count separately, and Kafka retention can reduce the value. **Silent drops do not appear in this metric.**
- `mill_tool_events_unreconciled` is a PostgreSQL count of reconciliation rows with `processed = false`. It includes failed sends, DLQ events, and silently dropped events; it is not calculated from Kafka offsets or DLQ length. **Silently dropped events increase this count without increasing the DLQ topic length**, demonstrating the observability gap.
- `mill_tool_events_average_staleness_seconds` is the average of `now - updated_at` over the current target row for each unique tool instance. With no target rows, Grafana shows no data rather than zero.

Kafka DLQ collection and PostgreSQL outcome collection each expose their own health metric, so an unavailable source is not presented as a healthy zero. The provisioned dashboard files are `base-streaming-platform.json` and `metrics-that-matter.json`; the latter is derived from the former with `python3 scripts/derive_metrics_that_matter_dashboard.py --check` verifying shared-panel parity and outcome-panel exclusivity.

## Inspect the stack

AKHQ is available at `http://localhost:8080`. To inspect the registered schemas:

```sh
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/mill-cuts-transactional-source-value/versions/latest
curl http://localhost:8081/subjects/mill-tool-events-source-value/versions/latest
```

### Pipeline topology graph

StreamLens runs at `http://localhost:5000` as part of the default stack. The topology snapshot refreshes about once a minute, so the graph can lag startup briefly; reload the page if it looks empty.

1. Open `http://localhost:5000`.
2. The `local` cluster is already listed and connected to the broker, Schema Registry, and Prometheus. Open it — no manual registration is needed.
3. In the topology view, expect producer nodes `mill-cuts-transactional-producer`, `mill-cuts-at-least-once-producer`, and `mill-tool-events-producer` with edges to their source topics (`mill-cuts-transactional-source`, `mill-cuts-atleastonce-source`, `mill-tool-events-source`), then on to consumer groups `mill-cuts-transactional-consumer`, `mill-cuts-idempotent-consumer`, and `mill-tool-events-consumer`.
4. Also expect schema nodes attached to the topics whose values use them, the `mill-cuts-committed` topic written by the transactional consumer, and the `mill-tool-events-dlq` topic.

Producers are detected from the `kafka_producer_topic_metrics_record_send_total` Prometheus series exported by the app JMX agent, so a producer appears once it has sent at least one record and stays listed while its process runs. StreamLens is read-only against Kafka: producing from its UI is disabled.

| Service | Address |
| --- | --- |
| Kafka from the host | `127.0.0.1:9092` |
| Kafka from Compose services | `broker:29092` |
| Schema Registry | `http://localhost:8081` |
| AKHQ | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| StreamLens | `http://localhost:5000` |
| PostgreSQL | `localhost:5432` |

PostgreSQL is published on loopback at `localhost:5432`; Compose services use `postgres:5432`. The local demo uses trust authentication and has no password. Example queries:

```sh
docker compose exec postgres psql --username gotogether --dbname gotogether \
  --command 'SELECT COUNT(*) FROM mill_cuts_transactional_writes;'
docker compose exec postgres psql --username gotogether --dbname gotogether \
  --command 'SELECT COUNT(*), COUNT(DISTINCT event_id) FROM mill_cuts_idempotent_writes;'
docker compose exec postgres psql --username gotogether --dbname gotogether \
  --command 'SELECT COUNT(*) FROM mill_tool_event_reconciliation WHERE processed = false;'
docker compose exec postgres psql --username gotogether --dbname gotogether \
  --command 'SELECT machine_id, tool_instance_id, event_id, updated_at FROM mill_tool_event_state ORDER BY machine_id, tool_instance_id;'
```

## Migration and backup

When upgrading from a previous version with retained PostgreSQL volumes, the cut table schema migration runs automatically during `docker compose up`. The migration is idempotent and deterministic:

1. **Before upgrading**, back up both cut tables:
   ```sh
   docker compose exec postgres pg_dump --username gotogether --dbname gotogether \
     --table mill_cuts_transactional_writes --table mill_cuts_idempotent_writes \
     > cut_tables_backup.sql
   ```

2. **Stop the running stack** (do not delete volumes):
   ```sh
   docker compose down
   ```

3. **Pull the new code and start the stack**:
   ```sh
   docker compose up -d --build
   docker compose ps -a
   python scripts/smoke_check.py
   ```

4. **Verify the migration**:
   ```sh
   docker compose exec postgres psql --username gotogether --dbname gotogether \
     --command 'SELECT COUNT(*), COUNT(DISTINCT event_id) FROM mill_cuts_transactional_writes;'
   docker compose exec postgres psql --username gotogether --dbname gotogether \
     --command 'SELECT COUNT(*), COUNT(DISTINCT event_id) FROM mill_cuts_idempotent_writes;'
   ```
   Both tables should have the same number of rows and unique `event_id` values. If the transactional table had duplicates before migration, the row count decreases to one per `event_id`.

5. **To roll back**, stop the stack, restore the backup, and revert the code:
   ```sh
   docker compose down
   docker compose exec postgres psql --username gotogether --dbname gotogether < cut_tables_backup.sql
   # Revert to the previous code version
   docker compose up -d --build
   ```

## Stop and reset

To stop services while retaining their named data volumes:

```sh
docker compose down
```

> **Destructive reset:** `docker compose down --volumes` permanently deletes this project's Kafka data, registered schemas, PostgreSQL data, Prometheus history, and Grafana data. Run it only when you intend to discard all demo state.
