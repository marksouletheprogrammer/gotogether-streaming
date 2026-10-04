# Exactly Once Semantics Specification

## Purpose

Provide a locally running comparison of Kafka transactional cut processing and replay-safe database persistence using canonical CNC milling cut records as part of the normal streaming platform startup.

## Requirements

### Requirement: Integrated cut pipeline lifecycle
The system SHALL start PostgreSQL, exactly two cut producers and two cut consumers (one per producer source topic), cut-topic/schema setup, and their monitoring alongside the existing Kafka, registry, AKHQ, Prometheus, and Grafana services using the documented default Docker Compose startup without profiles or modes. Setup MUST be repeatable without erasing existing data. The separate canonical sample topic SHALL remain empty and its registered JSON Schema SHALL remain unchanged; cut producers SHALL publish only to their own cut topics.

#### Scenario: Start the full stack
- **GIVEN** Docker Compose is available and the required host ports are free
- **WHEN** the documented default startup and readiness checks complete
- **THEN** both cut pipelines and PostgreSQL are running with the existing infrastructure, and the canonical sample topic remains empty with its original JSON Schema registered

#### Scenario: Restart without resetting state
- **GIVEN** the full stack has previously produced and stored cut data
- **WHEN** the documented default startup runs again without deleting volumes
- **THEN** cut-domain topics, registered Avro subjects, application services, and PostgreSQL are usable without wiping persisted data or adding redundant schema versions

### Requirement: Matched synthetic cut sources
The system SHALL operate two low-rate producers with distinct client IDs and source topics. They SHALL emit the same ordered, finite, deterministic series of `cut` records (including identical stable `event_id`, event time, tool instance, cut index, and measurement values), derived only from `DATA.md`; each decoded record MUST conform to its canonical JSON Schema, while each Kafka value MUST use the cut-derived Avro representation. The transactional-path producer SHALL enable Kafka idempotence; the other producer SHALL use at-least-once delivery without Kafka producer idempotence.

#### Scenario: Compare emitted series
- **GIVEN** both producers are configured for the same series length and emission interval
- **WHEN** the source topics are read to completion
- **THEN** both have the same ordered sequence of valid cut records at the configured low rate, on separate topics and using different producer client IDs

#### Scenario: Retry delivery
- **GIVEN** a temporary Kafka send failure
- **WHEN** either producer retries sending an outstanding cut
- **THEN** the idempotent producer does not create a second Kafka record for the retried send, while the at-least-once path makes no duplicate-prevention guarantee

### Requirement: Canonical cut Avro value contract
The system SHALL derive an Avro value schema for `cut` records from the canonical JSON Schema in `DATA.md`, preserving the envelope and cut payload field names and meanings without introducing other record kinds. Both cut source topics and the processed-cut topic SHALL use Schema Registry-framed Avro values and register the derived schema under their respective `<topic>-value` subjects as Avro, while the base sample subject `cnc-demo-records-value` remains the existing JSON Schema. Startup MUST reuse a matching registered schema without adding a version and MUST fail visibly without replacing a conflicting cut-topic subject. Because Avro does not enforce all canonical JSON Schema constraints, producers and consumers MUST reject invalid decoded cut values, including invalid timestamps, empty IDs, incorrect constants, and out-of-range measurements, before publishing, processing, or persisting them.

#### Scenario: Inspect registered values
- **GIVEN** the default stack has started and each cut topic contains a published cut
- **WHEN** a client retrieves each cut topic's value subject and decodes values using Schema Registry
- **THEN** each subject reports an Avro schema derived from the canonical cut shape, all three topics have decodable equivalent cut records, and the base sample subject remains JSON Schema

#### Scenario: Repeated registration
- **GIVEN** the cut topics already have matching Avro value subjects
- **WHEN** the default stack starts again without deleting persisted state
- **THEN** setup succeeds without registering additional schema versions or changing the existing subjects

#### Scenario: Conflicting registered cut schema
- **GIVEN** a cut topic's value subject contains a schema different from the derived canonical cut Avro schema
- **WHEN** the default startup attempts cut schema setup
- **THEN** setup fails without replacing that subject or producing cut records using the conflicting schema

#### Scenario: Reject an invalid cut
- **GIVEN** a cut has an empty ID, malformed event time, wrong constant, missing measurement, or negative measurement
- **WHEN** a producer or consumer validates the cut before publishing, processing, or storing it
- **THEN** it rejects the cut even if its Avro field types could otherwise encode it

### Requirement: Kafka transactional cut consumer
The first consumer SHALL read only the idempotent producer's source topic in its own consumer group, write decoded cuts directly to its PostgreSQL table, and publish each cut to a processed-cut Kafka topic. It SHALL commit the processed record and source offset together in a Kafka transaction only after the database write succeeds. Downstream Kafka observers SHALL use committed-read isolation so aborted output remains invisible. The Kafka transaction SHALL NOT be represented as including PostgreSQL; a replay after database commit can duplicate a row in the first table.

#### Scenario: Successful processing
- **GIVEN** the source topic contains a valid cut and PostgreSQL is available
- **WHEN** the first consumer writes that cut to its table and commits its Kafka transaction
- **THEN** the table contains the cut, a committed-read observer sees one processed Kafka record for that source record, and the consumer position advances atomically with the Kafka output

#### Scenario: Abort after database write before Kafka commit
- **GIVEN** the first consumer has committed the cut to PostgreSQL but has not committed its Kafka output and source offset
- **WHEN** its Kafka transaction aborts or the consumer crashes and then restarts
- **THEN** the attempted Kafka output and source offset are not committed, reprocessing eventually produces one committed output record, and the database table can contain an additional row for that cut

### Requirement: Separate PostgreSQL persistence guarantees
The system SHALL provide a PostgreSQL instance reachable from Compose services and from host localhost, with separate cut-domain tables for each pipeline. Each of the two consumers SHALL write decoded cuts directly from its own source topic to its own table; there SHALL be no separate processed-topic database sink. The first consumer SHALL write its database row before committing its Kafka output/offset transaction, permitting duplicate rows after replay. The second consumer SHALL use `event_id` as its table's primary key and upsert decoded cuts on that key, replacing stored values on a conflicting later delivery while keeping exactly one row per ID; it SHALL commit its own source offset only after the upsert succeeds. Neither pipeline SHALL claim a distributed Kafka/PostgreSQL atomic commit.

#### Scenario: Inspect side-by-side tables
- **GIVEN** both cut sources have emitted the same series and the consumers have caught up
- **WHEN** an operator connects to PostgreSQL locally and queries both tables
- **THEN** both tables are independently inspectable and contain records tied to the canonical cut IDs, with the second table containing at most one row per `event_id`

#### Scenario: Replay after database commit before Kafka offset commit
- **GIVEN** either consumer has committed a cut to PostgreSQL but has not committed its Kafka source offset
- **WHEN** that consumer restarts and reads the same cut again
- **THEN** the second consumer retains exactly one row for that `event_id` with the same values, while the first consumer may insert an additional row and is not presented as database exactly-once

#### Scenario: Upsert a changed cut with the same ID
- **GIVEN** the second consumer has stored a valid cut with a particular `event_id`
- **WHEN** it consumes a later valid cut with the same `event_id` but different values
- **THEN** its table contains exactly one row for that ID with the later cut's values, and the source offset advances only after the upsert succeeds

#### Scenario: Database unavailable
- **GIVEN** PostgreSQL rejects a write for either consumer
- **WHEN** that consumer processes the cut
- **THEN** it does not advance its source-group position past that cut; the first consumer does not commit Kafka output/offset, and processing retries when PostgreSQL becomes available

### Requirement: Cut pipeline monitoring on the existing dashboard
The system SHALL add live, distinguishable cut-pipeline panels to the existing localhost-accessible, provisioned Kafka/Grafana dashboard, using the existing Prometheus collection service rather than a separate dashboard or monitoring stack. Both cut producers MUST have separate throughput and error-rate time-series panels; each of the two consumer groups (the transactional consumer and the idempotent consumer) MUST have separate lag, processing-throughput, and processing-error-rate time-series panels. Dashboard selectors MUST filter producer panels by producer client ID and consumer panels by consumer group ID, default to all available values, and apply the selected value to every related panel. A topic selector MUST filter topic-labeled lag series. The view MUST retain existing broker and registry panels and distinguish an idle or caught-up component from missing or unavailable telemetry without manual dashboard import. It MUST use separate, readable legends, titles, and units for metrics with different meanings.

#### Scenario: Observe active cut pipelines
- **GIVEN** the default stack is running and metrics have been collected during a finite production run
- **WHEN** an operator opens the existing provisioned Kafka dashboard on localhost
- **THEN** broker and registry panels remain available, both producers have separately labeled throughput and error-rate panels, and both consumer groups have separately labeled lag, throughput, and error-rate panels

#### Scenario: Filter cut pipeline series
- **GIVEN** cut producer and consumer metrics are available for both pipeline identities
- **WHEN** the dashboard opens with its selectors set to all and an operator selects one producer client ID or one consumer group ID
- **THEN** all matching producer or consumer series are visible by default and only the selected identity's series appear in its related panels after filtering

#### Scenario: Observe idle and failed components
- **GIVEN** a producer has finished its finite run or a consumer group has caught up, and monitoring targets remain healthy
- **WHEN** an operator inspects the dashboard
- **THEN** idle throughput and errors are distinguishable from absent telemetry, and caught-up group lag is distinguishable from missing lag data

#### Scenario: Observe processing failure
- **GIVEN** a cut producer send or consumer processing/database write fails, or a metrics target becomes unreachable
- **WHEN** monitoring updates
- **THEN** the affected producer or consumer group has an observable error-rate indicator or unavailable-target indication, rather than appearing healthy and idle

### Requirement: Reproducible comparison and verification
The system SHALL provide short instructions to start, inspect, and stop the full Compose stack, including topic/group identifiers, local PostgreSQL access, each guarantee boundary, existing dashboard access, and nondestructive shutdown. Focused static and unit checks SHALL cover cut Avro schema derivation and round-trip decoding against the canonical JSON Schema, schema-valid matched records, Kafka transaction/offset ordering, replay-safe database writes, and producer/consumer-group metrics configuration. A single runtime smoke check SHALL verify default service startup, absence of unhandled startup errors in service logs, and reachability of Kafka, Schema Registry, PostgreSQL, the monitoring endpoints, and the expected topics; it SHALL NOT require a failure-injection integration suite.

#### Scenario: Verify default startup
- **GIVEN** a clean local environment with the documented prerequisites
- **WHEN** the static/unit checks and the single startup smoke check run against the default Compose stack
- **THEN** both producers and consumers, PostgreSQL, and existing infrastructure start without unhandled startup errors in their logs; Kafka, Schema Registry, PostgreSQL, the expected topics, and the existing Grafana/Prometheus endpoints are reachable, while the canonical sample topic remains empty
