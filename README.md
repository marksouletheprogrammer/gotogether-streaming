# GoTogether Streaming Platform

A local Docker Compose streaming demo for CNC cut data. The default stack starts
Kafka in single-node KRaft mode, Schema Registry, PostgreSQL, two producers, two
consumers, AKHQ, Prometheus, and Grafana. The producers each publish the same
finite 30-record Avro series at a two-second interval, then remain available for
metrics scraping. The canonical sample topic remains empty.

## Requirements

- Docker Engine or Docker Desktop with the Compose plugin; first startup needs
  internet access to pull pinned images and build the checksum-verified JMX
  exporter.
- Python 3.13 or newer for the contract and smoke checks.
- About 4 GiB of memory available to Docker and free localhost ports 5432, 8080,
  8081, 9090, 9092, and 3000.

## Static checks

From the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-test.txt
python3 scripts/extract_schema.py
python3 scripts/derive_cut_avro_schema.py --check
python -m unittest discover -s tests -v
```

`DATA.md` is the authoritative synthetic-data contract. The checked-in cut
Avro schema is derived from that contract; the base `cnc-demo-records` topic
continues to use its existing JSON Schema.

## Start and verify

```sh
source .venv/bin/activate
docker compose config
docker compose up -d --build
docker compose ps -a
python scripts/smoke_check.py
```

The smoke check verifies service state and startup logs, Kafka metadata and
required topics, Schema Registry subjects, the empty canonical sample topic,
PostgreSQL connectivity, AKHQ, Prometheus targets and live metrics, and the
provisioned Grafana dashboard. Setup jobs should exit with status 0; the
long-running services should be running, with Kafka, Schema Registry, and
PostgreSQL healthy.

Re-running `docker compose up -d --build` preserves existing volumes. Topic and
schema setup is repeatable and refuses conflicting schemas rather than replacing
them. The cut producers use their own topics, so their records do not change the
empty `cnc-demo-records` topic checked by the smoke test.

## Pipelines

```text
Transactional producer -> mill-cuts-transactional-source -> transactional consumer -> mill_cuts_transactional_writes
                                                            +-> mill-cuts-committed

At-least-once producer -> mill-cuts-replay-source -> idempotent consumer -> mill_cuts_idempotent_writes
```

The producers use distinct client IDs and source topics. The consumer groups are
`mill-cuts-transactional-consumer` and `mill-cuts-idempotent-consumer`.

Cut values use Schema Registry-framed Avro. The value subjects are:

- `mill-cuts-transactional-source-value`
- `mill-cuts-replay-source-value`
- `mill-cuts-committed-value`

The first consumer writes directly to PostgreSQL and commits its processed-topic
record and source offset in one Kafka transaction. PostgreSQL is outside that
Kafka transaction: a crash after the database commit but before the Kafka
transaction commits can cause the database row to be written again after replay.
The second consumer uses `event_id` as the table primary key and upserts on
redelivery. One row remains per ID; a later valid value with that ID replaces
the stored value.

Inspect the schemas and the existing dashboard:

```sh
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/mill-cuts-transactional-source-value/versions/latest
```

AKHQ is available at `http://localhost:8080`. The existing **Base Streaming
Platform** Grafana dashboard at `http://localhost:3000` retains its broker and
Schema Registry panels and adds producer throughput/error, consumer
throughput/error, and per-group lag panels.

| Service | Address |
| --- | --- |
| Kafka from the host | `127.0.0.1:9092` |
| Kafka from Compose services | `broker:29092` |
| Schema Registry | `http://localhost:8081` |
| AKHQ | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| PostgreSQL | `localhost:5432` |

## PostgreSQL

PostgreSQL is published on loopback at `localhost:5432`; Compose services use
`postgres:5432`. The local demo uses trust authentication and has no password.
To inspect both pipeline tables:

```sh
docker compose exec postgres psql --username gotogether --dbname gotogether \
  --command 'SELECT COUNT(*) FROM mill_cuts_transactional_writes;'
docker compose exec postgres psql --username gotogether --dbname gotogether \
  --command 'SELECT COUNT(*), COUNT(DISTINCT event_id) FROM mill_cuts_idempotent_writes;'
```

## Stop and reset

To stop services while retaining their named data volumes:

```sh
docker compose down
```

> **Destructive reset:** `docker compose down --volumes` permanently deletes
> this project's Kafka data, registered schemas, PostgreSQL data, Prometheus
> history, and Grafana data. Run it only when you intend to discard all demo
> state.
