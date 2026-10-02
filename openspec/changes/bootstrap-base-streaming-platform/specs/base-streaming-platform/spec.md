# Spec Delta

## Purpose

Provide a self-contained, locally operable streaming foundation for later CNC predictive-maintenance features without deploying any application that produces or consumes records.

## ADDED Requirements

### Requirement: Local stack lifecycle
The system SHALL provide a disposable Docker Compose stack that starts and stops the base services on one machine, with a single-node Kafka cluster in KRaft mode and no ZooKeeper dependency. The documented teardown SHALL remove this project's Compose-managed volumes so the next startup begins with fresh Kafka, schema registry, and monitoring data.

#### Scenario: Start from a clean checkout
- **GIVEN** Docker Compose is available and required host ports are free
- **WHEN** the documented startup command is run and services become ready
- **THEN** Kafka, schema registry, AKHQ, metrics collection, and Grafana are running and can reach their dependencies over the local Compose network

#### Scenario: Teardown discards demo state
- **GIVEN** the base stack is running with a sample topic, registered schema, and collected metrics
- **WHEN** the documented Compose teardown command is run and the stack is started again
- **THEN** previous topic data, registered schema versions, and monitoring history are absent, and a new empty sample topic and canonical schema are created

#### Scenario: Stop the stack
- **GIVEN** the base services are running
- **WHEN** the documented teardown command is run
- **THEN** the Compose-managed services stop and their demo data is discarded

### Requirement: Kafka connectivity and management
The system SHALL provide a locally reachable Kafka bootstrap endpoint and a host-local AKHQ UI for inspecting the cluster and managing its topics, without authentication in this local demonstration.

#### Scenario: Access cluster from host and containers
- **GIVEN** the base stack is ready
- **WHEN** a client connects using the documented host bootstrap endpoint or a service connects using the documented Compose-network bootstrap endpoint
- **THEN** the client can retrieve cluster metadata with broker addresses reachable from its own network

#### Scenario: Inspect Kafka with AKHQ
- **GIVEN** the base stack is ready
- **WHEN** a user opens the documented localhost AKHQ address
- **THEN** the Kafka cluster and the sample topic are visible without login

### Requirement: Canonical sample topic and schema
The system SHALL create an empty `cnc-demo-records` topic and register its value schema under a stable identifier in the local registry. The registered JSON Schema MUST retain the Draft 2020-12 envelope, three record kinds, field names, required fields, and validation constraints defined in `DATA.md`; `DATA.md` remains the sole authority for synthetic-data semantics. The base stack SHALL NOT publish sample records.

#### Scenario: Inspect initial topic and schema
- **GIVEN** the base stack has become ready on a clean checkout
- **WHEN** a user inspects `cnc-demo-records` and its registered value schema
- **THEN** the topic contains zero records and the retrievable registered schema matches the canonical JSON Schema in `DATA.md` without invented fields or altered semantics

#### Scenario: Repeat startup without resetting demo state
- **GIVEN** the sample topic and canonical value schema are already present in this project's Compose-managed state
- **WHEN** the documented startup command runs again without removing volumes
- **THEN** setup succeeds without recreating the topic or registering another schema version, and existing topic data and monitoring history remain intact

#### Scenario: Reject a conflicting persisted schema
- **GIVEN** `cnc-demo-records-value` is registered with a schema different from the canonical JSON Schema in `DATA.md`
- **WHEN** setup runs again without removing volumes
- **THEN** schema setup exits nonzero without registering a replacement or changing the existing subject

#### Scenario: Validate the canonical examples
- **GIVEN** the registered schema is retrieved
- **WHEN** the example `cut`, `assessment`, and `event` records from `DATA.md` are validated with date-time format checking enabled
- **THEN** all three are accepted, while a record with a missing required measurement or malformed timestamp is rejected

### Requirement: Preconfigured monitoring
The system SHALL collect live metrics from the local Kafka broker and schema registry, expose collection target health, and provide a Grafana instance on localhost with a pre-installed Kafka deployment dashboard. The dashboard SHALL include broker health/availability, throughput and request/latency or error indicators, and schema registry health/request or error indicators, displaying actual data from this stack rather than placeholder values.

#### Scenario: Open an active dashboard
- **GIVEN** the base stack and metrics pipeline are ready
- **WHEN** a user opens the documented localhost Grafana address and selects the provisioned dashboard
- **THEN** its data source and panels are available without manual import or login, and broker and registry targets report healthy with live time-series data

#### Scenario: Broker or registry becomes unreachable
- **GIVEN** metrics have been collected from both services
- **WHEN** collection for either the broker or the schema registry fails
- **THEN** the affected target is distinguishable as unavailable in the monitoring data instead of silently appearing healthy

### Requirement: Reproducible operation and verification
The system SHALL provide concise instructions for prerequisites, startup, host URLs, broker and registry endpoints, verifying topic/schema and dashboard health, and teardown. It SHALL provide automated checks for configuration and schema integrity and a runtime smoke check for the locally running stack.

#### Scenario: Verify a new setup
- **GIVEN** a clean checkout with the documented prerequisites
- **WHEN** a user runs the documented static checks, starts the stack, and runs the documented smoke check
- **THEN** checks report actionable failures or confirm readiness, empty-topic/schema registration, host UI reachability, and live broker/registry metrics

#### Scenario: Scope stays limited to the base platform
- **GIVEN** the base stack is started
- **WHEN** its managed services and sample topic are inspected
- **THEN** it contains no synthetic data generator, application producer or consumer, Kafka Streams job, or unused database/backend service
