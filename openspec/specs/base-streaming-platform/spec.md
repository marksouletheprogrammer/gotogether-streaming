# Base Streaming Platform Specification

## Purpose

Provide a self-contained, locally operable streaming platform for CNC predictive-maintenance and real-time data features, including the application services required by those features.

## Requirements

### Requirement: Local stack lifecycle
The system SHALL provide a Docker Compose stack that starts the infrastructure and required application services on one machine, with a single-node Kafka cluster in KRaft mode and no ZooKeeper dependency. Ordinary `docker compose down` SHALL stop services while retaining named volumes. The documented destructive reset SHALL use `docker compose down --volumes` and clearly warn that it permanently removes stored demo data.

#### Scenario: Start from a clean checkout
- **GIVEN** Docker Compose is available and required host ports are free
- **WHEN** the documented startup command is run and services become ready
- **THEN** Kafka, Schema Registry, PostgreSQL, the configured producers and consumers, AKHQ, metrics collection, and Grafana are running and can reach their dependencies over the local Compose network

#### Scenario: Teardown discards demo state
- **GIVEN** the base stack is running with a sample topic, registered schema, and collected metrics
- **WHEN** the documented destructive reset `docker compose down --volumes` is run and the stack is started again
- **THEN** previous topic data, PostgreSQL data, registered schema versions, and monitoring history are absent, and topics, tables, and canonical schemas are initialized again

#### Scenario: Stop the stack
- **GIVEN** the Compose-managed services are running
- **WHEN** `docker compose down` is run
- **THEN** the services stop and their named demo-data volumes are retained

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
The system SHALL collect live metrics from the Kafka broker, Schema Registry, both producers, both consumers, and both consumer groups, expose collection-target health, and provide one Grafana instance on localhost with a provisioned operational dashboard titled **CNC Streaming Operations**. The operational dashboard SHALL organize broker/registry, producer, consumer, and component-health information into clearly labeled sections. It SHALL display producer throughput and error rates in separate time-series panels, and display consumer throughput and lag in separate time-series panels; consumer processing errors SHALL NOT appear on the operational dashboard. Dashboard variables SHALL allow filtering producer panels by available producer client IDs and consumer panels by available consumer group IDs; each selector MUST default to all available values and apply consistently to the related panels. A topic selector SHALL filter panels whose metric series expose a topic label, including consumer lag, without implying that metrics lacking that label can be filtered by topic. Rates and lag MUST use appropriate units and readable legends. Each monitored component with a health signal SHALL have its own colored status panel: green for healthy, red for unhealthy, and a distinct unavailable/no-data state when health telemetry is missing. The dashboard MUST distinguish an idle or caught-up component from missing or unavailable telemetry and display live data from this stack rather than placeholders. Its existing Grafana UID SHALL remain stable.

#### Scenario: Open an active dashboard
- **GIVEN** the base stack and metrics pipeline are ready
- **WHEN** a user opens the documented localhost Grafana address and selects CNC Streaming Operations
- **THEN** its data source and panels are available without manual import, operational metrics are grouped into the defined sections, consumer processing errors are absent, and broker, registry, producer, consumer, and group-lag targets report healthy with live time-series data

#### Scenario: Filter producer and consumer metrics
- **GIVEN** metrics are available for multiple producer client IDs, consumer group IDs, and topic-labeled lag series
- **WHEN** the dashboard opens with default selector values and an operator selects one producer, one consumer group, or one topic
- **THEN** the relevant producer throughput/error, consumer throughput/lag, or topic-labeled lag panels show all matching series by default and only the selected series after filtering

#### Scenario: Broker or registry becomes unreachable
- **GIVEN** metrics have been collected from both services
- **WHEN** collection for either the broker or the Schema Registry fails
- **THEN** that component's health panel reports unhealthy or unavailable according to its target-health signal, not healthy, and unrelated component panels remain independently readable

#### Scenario: Observe idle, caught-up, and missing telemetry
- **GIVEN** a producer is temporarily idle or a consumer group has caught up while its monitoring target is healthy
- **WHEN** an operator inspects the dashboard
- **THEN** zero throughput/errors and zero lag remain distinguishable from missing or unavailable telemetry, and component health is shown independently in its own status panel

### Requirement: Reproducible operation and verification
The system SHALL provide concise instructions for prerequisites, default startup, both producer-topic-consumer-table paths, host URLs, Kafka and Schema Registry subjects, PostgreSQL tables, consumer groups, the shared Grafana dashboard, and retained-data teardown. It SHALL provide automated checks for configuration and schema integrity and one runtime smoke check covering service startup, logs, topic/schema/database and dashboard reachability.

#### Scenario: Verify a new setup
- **GIVEN** a clean checkout with the documented prerequisites
- **WHEN** a user runs the documented static checks, starts the stack, and runs the documented smoke check
- **THEN** checks report actionable failures or confirm running services, clean startup logs, the empty canonical sample topic, registered JSON/Avro schemas, PostgreSQL and host UI reachability, and live pipeline metrics