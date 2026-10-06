# Spec Delta

## MODIFIED Requirements

### Requirement: Canonical and varied technician events
The event producer SHALL continuously generate paced, valid `kind: event` records using only the envelope and event payload defined in `DATA.md`, including stable nonempty `event_id`, `machine_id: mill-1`, `occurred_at`, and `tool_instance_id`. Default pacing SHALL be 200 milliseconds per record, with a configurable positive interval; it SHALL continue past 30 records until orderly shutdown. It SHALL include different tool instances and recurring events for some of the same tool instances (installation before inspection); each new event SHALL have a unique ID. The source topic and dead-letter topic MUST carry Schema Registry-framed Avro values derived from the canonical `event` variant and registered under their own `<topic>-value` subjects. Producer and consumer MUST reject invalid event values that Avro typing alone permits; startup MUST reuse matching subjects and reject conflicting subjects without replacing them. The canonical sample subject MUST remain JSON Schema.

#### Scenario: Mixed new and repeated tool instances
- **GIVEN** a clean run of the event producer
- **WHEN** its source topic is decoded after more than 30 events and inspected again later
- **THEN** the topic continues to grow, at least two tool instances have records, at least one tool instance has multiple records with distinct event IDs, inspections follow installations, and each decoded record satisfies the `DATA.md` event constraints

#### Scenario: Stop continuous event generation
- **GIVEN** the event producer is still publishing after the former 30-record limit
- **WHEN** the stack is stopped normally
- **THEN** it stops publishing without a partial event and its already committed Kafka records remain available

#### Scenario: Check wire format and schema compatibility
- **GIVEN** both event topics have been initialized
- **WHEN** their registered value subjects and published values are inspected
- **THEN** both subjects are Avro and their records decode as canonical events, while existing cut subjects and the canonical sample JSON Schema are unchanged

#### Scenario: Invalid or conflicting event schema
- **GIVEN** an invalid event value or a persisted event subject incompatible with the canonical derived schema
- **WHEN** event publication/consumption or startup schema verification runs
- **THEN** invalid values are rejected before persistence or publication and setup refuses to overwrite the conflicting subject

### Requirement: Dead-letter handling for simulated processing failures
The event consumer SHALL support a configurable probability of randomly injecting a per-message DLQ processing failure, including a zero-failure setting for repeatable checks. This outcome SHALL be mutually exclusive with simulated silent loss. For a DLQ failure it SHALL publish the original Avro event to the event dead-letter topic with the error description in Kafka headers and leave its reconciliation row unprocessed, without writing a DLQ status to that table. It MUST commit the source offset only after dead-letter publication is confirmed. A failed DLQ publication MUST leave the source offset retryable, not silently drop the record; DLQ redelivery MAY yield duplicate Kafka DLQ records. A transient database failure during successful processing MUST leave the source offset retryable rather than be mislabeled as a simulated failure.

#### Scenario: Inject a processing failure
- **GIVEN** the configured DLQ failure probability selects a valid source event
- **WHEN** the consumer handles that event
- **THEN** the DLQ contains the original decodable event with an error header, the reconciliation row remains unprocessed, and no target update or silent-drop outcome is claimed for that event

#### Scenario: Dead-letter delivery is unavailable
- **GIVEN** an event has been selected for simulated DLQ failure and the DLQ publish is not confirmed
- **WHEN** the consumer handles that event
- **THEN** its source offset is not committed and it remains eligible for retry, not silently lost

#### Scenario: Processing succeeds without injection
- **GIVEN** a valid event and both simulated failure probabilities are zero
- **WHEN** the consumer handles it
- **THEN** it writes the target and processed marker without publishing that event to the DLQ

## ADDED Requirements

### Requirement: Simulated silent loss alongside dead-letter failures
The event consumer SHALL support a separately configurable probability of simulated silent loss, defaulting to 0.05 of all eligible unprocessed source events, alongside the existing default DLQ-failure probability of 0.2; both settings MUST accept zero and MUST reject invalid values or a combined probability greater than one. The outcomes SHALL be mutually exclusive: processing success, DLQ failure, or silent loss. For a silent loss the consumer SHALL commit the source offset without writing to the target or DLQ and without marking the existing reconciliation row processed; it SHALL NOT claim successful persistence. This simulation MUST NOT convert actual database, validation, or DLQ-publication failures into silent losses. The unreconciled count SHALL include silently lost events, while DLQ topic length SHALL not include them.

#### Scenario: Simulate an unreported dropped event
- **GIVEN** a valid, not-yet-processed technician event has a committed reconciliation row and the silent-loss selection chooses it
- **WHEN** the event consumer handles it
- **THEN** the source offset advances, the reconciliation row remains `processed = false`, no target row changes, and no corresponding DLQ record or error header is published

#### Scenario: Observe silent loss separately from DLQ count
- **GIVEN** one event was silently dropped, a different event was dead-lettered, and both have unprocessed reconciliation rows
- **WHEN** the independently sourced unreconciled and DLQ dashboard counters are refreshed
- **THEN** unreconciled count includes both events while DLQ topic length counts only the dead-lettered record

#### Scenario: Disable injected outcomes
- **GIVEN** both failure probabilities are configured as zero
- **WHEN** a valid unprocessed event is consumed and the database is available
- **THEN** it is processed, its reconciliation row is marked processed, and neither simulated failure branch is used

#### Scenario: Preserve retryability for real failures
- **GIVEN** persistence fails or a selected dead-letter publication is not confirmed
- **WHEN** the consumer handles a source event
- **THEN** it does not silently drop the event or commit the source offset before the required durable operation succeeds
