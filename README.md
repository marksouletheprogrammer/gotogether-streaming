# GoTogether Streaming Platform

A local Docker Compose streaming demo for CNC cut data and technician events. The default stack starts Kafka in single-node KRaft mode, Schema Registry, PostgreSQL, three producers, three consumers, a lag/DLQ exporter, an event outcome exporter, AKHQ, Prometheus, and Grafana. The two cut producers each publish the same finite 30-record Avro series at a two-second interval; the event producer publishes its own low-rate event series. Producer and exporter processes remain available for metric scraping. The canonical sample topic remains empty.

## Requirements

- Docker Engine or Docker Desktop with the Compose plugin; first startup needs internet access to pull pinned images and build the checksum-verified JMX exporter.
- Python 3.13 or newer for the contract and smoke checks.
- About 4 GiB of memory available to Docker and free localhost ports 5432, 8080, 8081, 9090, 9092, and 3000.

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

## Pipelines

```text
Transactional cut producer -> mill-cuts-transactional-source -> transactional cut consumer -> mill_cuts_transactional_writes
                                                                    +-> mill-cuts-committed
At-least-once cut producer -> mill-cuts-replay-source -> idempotent cut consumer -> mill_cuts_idempotent_writes
Technician-event producer -> mill-tool-events-source -> mill-tool-events-consumer -> mill_tool_event_state
                                                                                     +-> mill-tool-events-dlq
```

The cut producer client IDs are `mill-cuts-transactional-producer` and `mill-cuts-at-least-once-producer`; their consumer groups are `mill-cuts-transactional-consumer` and `mill-cuts-idempotent-consumer`.

The technician-event producer is `mill-tool-events-producer`; its consumer group is `mill-tool-events-consumer`. It emits canonical `event` records from `DATA.md` for multiple tool instances, including repeated inspections for the same tool. Both event Kafka topics use Schema Registry-framed Avro values:

- `mill-tool-events-source-value`
- `mill-tool-events-dlq-value`

The event producer commits one row to `mill_tool_event_reconciliation` before sending each event. If that database commit fails, it does not send. A Kafka send failure leaves the row with `processed = false`; there is no automatic publisher outbox or background resend. A later manual restart can send the same deterministic event again. The consumer uses the row's `processed` flag to skip reapplying a successfully handled event.

On successful processing, the consumer updates the latest state for a `(machine_id, tool_instance_id)` and marks that event's reconciliation row processed in one PostgreSQL transaction. Kafka offset commits happen after that database transaction; Kafka and PostgreSQL do not share a distributed transaction. The target table's `updated_at` records the time the target was successfully updated.

`EVENT_FAILURE_PROBABILITY` defaults to `0.2` and can be set to `0` for deterministic success checks or `1` for deterministic DLQ checks. An injected failure publishes the original Avro event to `mill-tool-events-dlq` with an `error` header before committing the source offset; it does not update the target or mark the reconciliation row processed. Inspect the DLQ topic and its error header in AKHQ. DLQ messages are not automatically replayed.

### Metrics that matter

Grafana provisions two dashboards at `http://localhost:3000`:

- **Base Streaming Platform** shows broker, Schema Registry, cut, and conventional event producer/consumer throughput, errors, and lag.
- **Metrics That Matter** contains those same baseline panels plus the three outcome metrics below.

These metrics have separate sources and meanings:

- `mill_tool_events_dlq_topic_length` is the retained record count from the Kafka DLQ topic, summed as latest offset minus earliest retained offset across its partitions. Duplicate DLQ records count separately, and Kafka retention can reduce the value.
- `mill_tool_events_unreconciled` is a PostgreSQL count of reconciliation rows with `processed = false`. It includes failed sends and DLQ events; it is not calculated from Kafka offsets or DLQ length.
- `mill_tool_events_average_staleness_seconds` is the average of `now - updated_at` over the current target row for each unique tool instance. With no target rows, Grafana shows no data rather than zero.

Kafka DLQ collection and PostgreSQL outcome collection each expose their own health metric, so an unavailable source is not presented as a healthy zero. The provisioned dashboard files are `base-streaming-platform.json` and `metrics-that-matter.json`; the latter is derived from the former with `python3 scripts/derive_metrics_that_matter_dashboard.py --check` verifying panel parity.

## Inspect the stack

AKHQ is available at `http://localhost:8080`. To inspect the registered schemas:

```sh
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/mill-cuts-transactional-source-value/versions/latest
curl http://localhost:8081/subjects/mill-tool-events-source-value/versions/latest
```

| Service | Address |
| --- | --- |
| Kafka from the host | `127.0.0.1:9092` |
| Kafka from Compose services | `broker:29092` |
| Schema Registry | `http://localhost:8081` |
| AKHQ | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
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

## Stop and reset

To stop services while retaining their named data volumes:

```sh
docker compose down
```

> **Destructive reset:** `docker compose down --volumes` permanently deletes this project's Kafka data, registered schemas, PostgreSQL data, Prometheus history, and Grafana data. Run it only when you intend to discard all demo state.
