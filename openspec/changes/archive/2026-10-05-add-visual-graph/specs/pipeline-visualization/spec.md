# Spec Delta

## Purpose

Give users an interactive topology graph of the streaming platform so they can see every pipeline end to end, as an addition to AKHQ and Grafana.

## ADDED Requirements

### Requirement: Visualization service on default startup
The system SHALL run StreamLens in Docker as part of the ordinary Docker Compose startup and expose its web UI on the host at `http://localhost:5000` without authentication. It MUST be bound to loopback like the other web tools, MUST start after the services it reads from are ready, and MUST NOT require manual cluster registration.

#### Scenario: Start the full stack
- **GIVEN** a clean checkout with Docker Compose available
- **WHEN** the default startup completes
- **THEN** the StreamLens service is running and healthy and `http://localhost:5000` serves its UI

#### Scenario: Cluster is pre-registered
- **GIVEN** the stack has started
- **WHEN** the user opens StreamLens
- **THEN** this project's Kafka cluster is already listed, connected to its broker and Schema Registry, and can be opened without entering any configuration

#### Scenario: Restart
- **GIVEN** the stack was started before
- **WHEN** the default startup runs again, with or without removing volumes
- **THEN** StreamLens starts with the same pre-registered cluster

### Requirement: End-to-end pipeline topology
The topology shown by StreamLens SHALL include, for every pipeline in the project, its producer(s) identified by client ID, its source topic(s), its Avro/JSON schema subject(s), and its consumer group(s), with edges showing the direction of data flow. This includes the transactional and at-least-once cut pipelines, the technician-event pipeline and its dead-letter topic, and the committed topic written by the transactional consumer.

#### Scenario: Cut pipelines visible
- **GIVEN** the stack is running and the cut producers have sent records
- **WHEN** the user views the cluster topology
- **THEN** `mill-cuts-transactional-producer` connects to `mill-cuts-transactional-source` and on to consumer group `mill-cuts-transactional-consumer`, and `mill-cuts-at-least-once-producer` connects to `mill-cuts-replay-source` and on to consumer group `mill-cuts-idempotent-consumer`

#### Scenario: Event pipeline visible
- **GIVEN** the stack is running and the event producer has sent records
- **WHEN** the user views the cluster topology
- **THEN** `mill-tool-events-producer` connects to `mill-tool-events-source` and on to consumer group `mill-tool-events-consumer`, and the `mill-tool-events-dlq` topic is shown

#### Scenario: Schemas visible
- **GIVEN** the cut and event schemas are registered
- **WHEN** the user views the topology
- **THEN** schema nodes are attached to the topics whose values use them

#### Scenario: Producers persist after finite series
- **GIVEN** a producer has finished its finite record series but its process is still running
- **WHEN** the user refreshes the topology
- **THEN** that producer remains shown

### Requirement: Producer discoverability
The system SHALL publish, for each application producer, the number of records sent per topic labelled with the producer's client ID and topic to the project's Prometheus, so the visualization can attribute producers to topics by client ID. Existing metric series, Prometheus scrape targets, and Grafana dashboards MUST remain unchanged.

#### Scenario: Per-producer, per-topic series
- **GIVEN** a producer has sent at least one record
- **WHEN** Prometheus is queried for `kafka_producer_topic_metrics_record_send_total`
- **THEN** a series exists with `client_id` equal to the producer's client ID and `topic` equal to the topic it wrote to

#### Scenario: Existing monitoring unaffected
- **GIVEN** the change is applied
- **WHEN** the existing Prometheus targets and both Grafana dashboards are loaded
- **THEN** all targets scrape successfully and dashboard definitions are byte-for-byte unchanged

### Requirement: Additive to existing tools
StreamLens MUST be an additional tool. AKHQ, Grafana (both dashboards), Prometheus, and all pipelines SHALL keep their existing addresses and behavior, and StreamLens MUST NOT write to any topic, schema subject, or database, nor enable producing messages from its UI.

#### Scenario: Existing tools unchanged
- **GIVEN** the stack is running with StreamLens
- **WHEN** the user opens AKHQ at `http://localhost:8080` and Grafana at `http://localhost:3000`
- **THEN** they work as before with the same dashboards and data

#### Scenario: Read-only against Kafka
- **GIVEN** the pre-configured cluster
- **WHEN** the cluster configuration is inspected
- **THEN** produce-from-UI is disabled

### Requirement: Usage documentation
The README SHALL list the StreamLens address in the service table and give step-by-step instructions for opening the graph and what to look for, consistent with the existing documentation style.

#### Scenario: Find StreamLens instructions
- **GIVEN** a user reading the README
- **WHEN** they look for the topology graph
- **THEN** they find the address `http://localhost:5000`, how to open the pre-configured cluster, and the expected pipelines
