# GoTogether Streaming Platform

A disposable local Kafka foundation for demonstrating streaming concepts. The stack contains a single-node KRaft broker, Schema Registry, AKHQ, Prometheus, and Grafana. It has no producers, consumers, Kafka Streams jobs, or database services and publishes no sample records.

## Requirements

- Docker Engine or Docker Desktop with the Compose plugin; first startup needs internet access to pull pinned images and build the checksum-verified JMX exporter images.
- Python 3.13 or newer for the contract and smoke checks.
- About 4 GiB of memory available to Docker and free localhost ports 9092, 8080, 8081, 9090, and 3000.

## Static checks

From the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-test.txt
python3 scripts/extract_schema.py
python -m unittest discover -s tests -v
```

The extractor deterministically generates `schemas/cnc-demo-records.schema.json` from the canonical schema block in `DATA.md`. The tests validate the three documented examples with date-time format checking, reject invalid records, and check schema and Compose/monitoring configuration contracts. `DATA.md` is the authoritative data contract and is not modified by the extractor.

## Start and verify

```sh
source .venv/bin/activate
docker compose config
docker compose up -d --build
docker compose ps -a
python scripts/smoke_check.py
```

The smoke check verifies host and in-container Kafka metadata, the empty topic, the registered schema, AKHQ topic visibility, Prometheus targets and live service metrics, Grafana health and dashboard provisioning, and that no application producer/consumer/database services are present. Setup jobs should exit with status 0; the long-running services should become healthy/running.

Re-running `docker compose up -d --build` preserves existing volumes. Bootstrap leaves an existing topic and matching schema intact, but fails rather than replacing a different registered schema. The smoke check's zero-offset assertion is for a fresh demo topic; records written later remain in Kafka and make that check fail until the demo state is intentionally reset.

| Service | Local address or endpoint |
| --- | --- |
| Kafka from the host | `127.0.0.1:9092` |
| Kafka from Compose services | `broker:29092` |
| Schema Registry from the host | `http://localhost:8081` |
| Schema Registry from Compose services | `http://schema-registry:8081` |
| AKHQ | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |

AKHQ and Grafana are unauthenticated local demonstration interfaces and are bound only to the host loopback interface. Grafana's provisioned dashboard is **Base Streaming Platform**. The initial topic is `cnc-demo-records`, with one partition and zero records. Its value schema is registered as the JSON Schema subject `cnc-demo-records-value` and matches the Draft 2020-12 contract in `DATA.md`.

## Stop and reset

To stop the services while retaining their named data volumes:

```sh
docker compose down
```

> **Destructive reset:** `docker compose down --volumes` permanently deletes this project's Kafka data, registered schemas, Prometheus history, and Grafana data. Run it only when you intend to discard all demo state.

To start again with fresh demo state after that reset, run `docker compose up -d --build` and repeat the verification commands above.
