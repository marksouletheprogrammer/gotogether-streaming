# Metrics That Matter Specification

## Purpose

Demonstrate the Kafka dead-letter topic's retained length, whether technician events have been applied to PostgreSQL, and how old each entity's latest stored update is, alongside conventional streaming metrics.

## Requirements

### Requirement: Independent event pipeline on default startup
The system SHALL start one technician-`event` producer, one source topic, one consumer in a distinct group, a dead-letter topic, required topic/schema/database setup, and monitoring in parallel with the existing services under ordinary Docker Compose startup. The new pipeline MUST NOT publish to the cut topics or the empty canonical sample topic, change their registered schemas, alter the two cut pipelines' data or guarantees, or erase persisted state on repeated startup.

#### Scenario: Start the full stack
- **GIVEN** a clean checkout with Docker Compose available
- **WHEN** the default startup and readiness checks complete
- **THEN** the event producer and consumer run with the existing cut pipelines, both event topics and tables exist, and the canonical sample topic remains empty

#### Scenario: Restart with retained data
- **GIVEN** the event pipeline has published and persisted events
- **WHEN** the default startup runs again without removing volumes
- **THEN** existing event and cut data remain accessible, and event setup does not add duplicate schema versions or discard reconciliation state

### Requirement: Canonical and varied technician events
The event producer SHALL generate low-rate, valid `kind: event` records using only the envelope and event payload defined in `DATA.md`, including stable nonempty `event_id`, `machine_id: mill-1`, `occurred_at`, and `tool_instance_id`. It SHALL include different tool instances and recurring events for some of the same tool instances (installation before inspection); each new event SHALL have a unique ID. The source topic and dead-letter topic MUST carry Schema Registry-framed Avro values derived from the canonical `event` variant and registered under their own `<topic>-value` subjects. Producer and consumer MUST reject invalid event values that Avro typing alone permits; startup MUST reuse matching subjects and reject conflicting subjects without replacing them. The canonical sample subject MUST remain JSON Schema.

#### Scenario: Mixed new and repeated tool instances
- **GIVEN** a clean run of the event producer
- **WHEN** its source topic is decoded
- **THEN** at least two tool instances have records, at least one tool instance has multiple records with distinct event IDs, inspections follow installations, and each decoded record satisfies the `DATA.md` event constraints

#### Scenario: Check wire format and schema compatibility
- **GIVEN** both event topics have been initialized
- **WHEN** their registered value subjects and published values are inspected
- **THEN** both subjects are Avro and their records decode as canonical events, while existing cut subjects and the canonical sample JSON Schema are unchanged

#### Scenario: Invalid or conflicting event schema
- **GIVEN** an invalid event value or a persisted event subject incompatible with the canonical derived schema
- **WHEN** event publication/consumption or startup schema verification runs
- **THEN** invalid values are rejected before persistence or publication and setup refuses to overwrite the conflicting subject

### Requirement: Durable per-message reconciliation and current entity state
Before first publishing an event ID, the producer SHALL commit one reconciliation row keyed by `event_id` and initialized with `processed = false`; before every send it SHALL ensure that row exists without creating a second row or resetting an already processed row. If that database commit fails, the producer MUST NOT send the event. A send failure after the database commit SHALL leave an unprocessed row and MUST be reported; automatic replay of unconfirmed sends is NOT required. For a successfully handled event, the consumer SHALL atomically upsert the latest event for its `(machine_id, tool_instance_id)` into a separate target table with a non-null `updated_at` timestamp and mark that event's reconciliation row `processed = true` in the same PostgreSQL transaction. A source offset MUST NOT be committed before durable success; replay of a processed event MUST NOT create extra rows or refresh its `updated_at`.

#### Scenario: Publish then consume
- **GIVEN** a valid new technician event
- **WHEN** the producer commits its reconciliation row, publishes the event, and the consumer processes it
- **THEN** exactly one reconciliation row exists for its `event_id` with `processed = true` and the entity target row reflects that event with an `updated_at` set at successful persistence

#### Scenario: Send fails after reconciliation commit
- **GIVEN** the producer has committed a reconciliation row with `processed = false`
- **WHEN** its Kafka send fails
- **THEN** the producer reports the error and the single reconciliation row remains unprocessed, without a guarantee of automatic resend

#### Scenario: Database commit followed by consumer replay
- **GIVEN** a source offset was not committed after the target update and processed marker committed together
- **WHEN** the consumer replays that event
- **THEN** the event remains processed, the target contains one row for that entity, and its `updated_at` does not move solely because of replay

#### Scenario: Database unavailable
- **GIVEN** PostgreSQL cannot durably write either the reconciliation row or the consumer's target-and-marker transaction
- **WHEN** the corresponding producer or consumer handles an event
- **THEN** the producer does not send an event without a committed reconciliation row and the consumer does not commit the source offset or claim the event was processed

### Requirement: Dead-letter handling for simulated processing failures
The event consumer SHALL support a configurable probability of randomly injecting a per-message processing failure, including a zero-failure setting for repeatable checks. For such a failure it SHALL publish the original Avro event to the event dead-letter topic with the error description in Kafka headers and leave its reconciliation row unprocessed, without writing a DLQ status to that table. It MUST commit the source offset only after dead-letter publication is confirmed. A failed DLQ publication MUST leave the source offset retryable, not silently drop the record; DLQ redelivery MAY yield duplicate Kafka DLQ records. A transient database failure during successful processing MUST leave the source offset retryable rather than be mislabeled as a simulated failure.

#### Scenario: Inject a processing failure
- **GIVEN** the configured failure probability selects a valid source event
- **WHEN** the consumer handles that event
- **THEN** the DLQ contains the original decodable event with an error header, the reconciliation row remains unprocessed, and no target update is claimed for that event

#### Scenario: Dead-letter delivery is unavailable
- **GIVEN** an event has been selected for simulated failure and the DLQ publish is not confirmed
- **WHEN** the consumer handles that event
- **THEN** its source offset is not committed and it remains eligible for retry

#### Scenario: Processing succeeds without injection
- **GIVEN** a valid event and zero configured failure probability
- **WHEN** the consumer handles it
- **THEN** it writes the target and processed marker without publishing that event to the DLQ

### Requirement: Two provisioned views of operational metrics
The existing Grafana dashboard SHALL retain its broker, registry, cut, and event metrics while presenting producer throughput, producer error rate, consumer throughput, consumer error rate, and consumer lag in separate time-series panels. Producer throughput/error panels SHALL be filterable by producer client ID; consumer throughput/error/lag panels SHALL be filterable by consumer group ID. The corresponding selectors MUST default to all available values and apply consistently to all related panels. Topic selection MUST filter panels whose metric series expose a topic label, including lag, without treating unlabeled producer metrics as topic-labeled. A second provisioned dashboard SHALL reproduce every shared panel and selector from the base dashboard with identical shared-panel definitions and only a distinct identity/title, plus exactly three outcome panels that remain exclusive to the second dashboard: DLQ topic length (sum across DLQ partitions of latest offset minus earliest retained offset), unreconciled count (reconciliation rows with `processed = false`), and average staleness in seconds (arithmetic mean of `now - updated_at` over the latest target row per unique `(machine_id, tool_instance_id)`). DLQ length SHALL be displayed as a single-value counter/stat, come from Kafka, count retained copies including duplicates, and vary with topic retention; it SHALL NOT be inferred from reconciliation rows. Unreconciled count SHALL be displayed as a single-value counter/stat, come from PostgreSQL, include rows for failed sends and DLQ events, and SHALL NOT be inferred from Kafka offsets. Average staleness SHALL remain a time-series value with seconds as its unit. On zero target rows staleness SHALL be shown as no data rather than a fabricated zero; zero Kafka topic length and zero unreconciled rows SHALL be shown as zero when their respective collection sources are healthy. A down Kafka or database metrics source MUST be distinguishable from a healthy zero reading without substituting one source's data for the other.

#### Scenario: Compare dashboards
- **GIVEN** a live event pipeline and both dashboards provisioned on localhost
- **WHEN** an operator opens both dashboards
- **THEN** each shows identical shared broker, registry, cut, and event panels and selectors, while only the second shows DLQ topic length, unreconciled count, and average staleness

#### Scenario: Inspect separate DLQ and reconciliation counts
- **GIVEN** an unprocessed row for a failed send, another unprocessed row whose event has two retained DLQ copies, and a processed row
- **WHEN** outcome metrics are refreshed
- **THEN** DLQ topic length is shown as a counter/stat with value two retained Kafka records and unreconciled count as a counter/stat with value two database rows; neither metric is derived from the other's source

#### Scenario: Observe staleness and unavailable telemetry
- **GIVEN** at least two entity rows with different `updated_at` timestamps
- **WHEN** time advances without another successful update and metrics are scraped
- **THEN** average staleness increases as a seconds-based time series according to the mean of those two entity ages; if the target table is empty or its measurement fails, the panel reports no data rather than a healthy zero

#### Scenario: Kafka DLQ metrics unavailable
- **GIVEN** Kafka cannot provide DLQ partition offsets
- **WHEN** monitoring refreshes
- **THEN** DLQ topic length is shown as unavailable, not zero, without altering the independent database-derived unreconciled and staleness measurements

#### Scenario: Dashboard filters and shared-panel parity
- **GIVEN** both dashboards are provisioned with producer, consumer-group, and topic-labeled metrics
- **WHEN** an operator changes any selector on either dashboard
- **THEN** its matching shared panels respond to the same selection rules, selectors default to all values, and contract checks confirm that all shared panels remain identical while outcome panels remain exclusive to the second dashboard

### Requirement: Reproducible event-pipeline verification
The system SHALL document event topic/group and table identifiers, both Grafana dashboards, nondestructive Compose startup/shutdown, the meaning and independent sources of the three outcome metrics, and how to inspect DLQ errors. Focused automated checks SHALL cover canonical event Avro equivalence/validation, installation-and-repeat generation, database-before-send ordering, atomic target-and-marker updates, DLQ header and offset ordering, Kafka DLQ topic length, and independent PostgreSQL outcome counts. The startup smoke check SHALL verify the new services, topics, subjects, tables, metrics targets, and two provisioned dashboards while preserving the existing cut and empty-sample checks.

#### Scenario: Verify locally
- **GIVEN** the documented development dependencies and ordinary stack are available
- **WHEN** static/unit checks and the startup smoke check run
- **THEN** they confirm event functionality and two dashboards without requiring destructive reset or altering the existing cut comparison
