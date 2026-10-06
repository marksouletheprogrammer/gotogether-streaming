# Spec Delta

## MODIFIED Requirements

### Requirement: End-to-end pipeline topology
The topology shown by StreamLens SHALL include, for every pipeline in the project, its producer(s) identified by client ID, its source topic(s), its Avro/JSON schema subject(s), and its consumer group(s), with edges showing the direction of data flow. This includes the transactional and at-least-once cut pipelines, the technician-event pipeline and its dead-letter topic, and the committed topic written by the transactional consumer.

#### Scenario: Cut pipelines visible
- **GIVEN** the stack is running and the cut producers have sent records
- **WHEN** the user views the cluster topology
- **THEN** `mill-cuts-transactional-producer` connects to `mill-cuts-transactional-source` and on to consumer group `mill-cuts-transactional-consumer`, and `mill-cuts-at-least-once-producer` connects to `mill-cuts-atleastonce-source` and on to consumer group `mill-cuts-idempotent-consumer`

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
