# Spec Delta

## MODIFIED Requirements

### Requirement: Matched synthetic cut sources
The system SHALL operate two continuously publishing, paced producers with distinct client IDs and source topics. They SHALL emit the same ordered, unbounded, deterministic sequence of `cut` records indexed by completed cut (including identical stable `event_id`, event time, tool instance, cut index, and measurement values), derived only from `DATA.md`; each decoded record MUST conform to its canonical JSON Schema, while each Kafka value MUST use the cut-derived Avro representation. Default pacing SHALL be 200 milliseconds per record per producer, with a configurable positive interval; both producers SHALL continue past 30 records and run until orderly shutdown. The transactional-path producer SHALL enable Kafka idempotence; the other producer SHALL use at-least-once delivery without Kafka producer idempotence.

#### Scenario: Compare emitted series
- **GIVEN** both producers start with the same sequence configuration and interval
- **WHEN** their source topics are inspected after more than 30 indexed cuts and again later without restarting
- **THEN** both topics continue to grow and their corresponding indexed records contain the same valid cut fields and values in order, on separate topics with different producer client IDs

#### Scenario: Controlled shutdown
- **GIVEN** both cut producers are publishing continuously
- **WHEN** the stack is stopped normally
- **THEN** both producers stop without publishing partial records and their already committed Kafka records remain available

#### Scenario: Retry delivery
- **GIVEN** a temporary Kafka send failure
- **WHEN** either producer retries sending an outstanding cut
- **THEN** the idempotent producer does not create a second Kafka record for the retried send, while the at-least-once path makes no duplicate-prevention guarantee

### Requirement: Kafka transactional cut consumer
The first consumer SHALL read only the idempotent producer's source topic in its own consumer group, upsert decoded cuts by `event_id` directly into its PostgreSQL table, and publish each cut to a processed-cut Kafka topic. It SHALL commit the processed record and source offset together in a Kafka transaction only after the database write succeeds. Downstream Kafka observers SHALL use committed-read isolation so aborted output remains invisible. The Kafka transaction SHALL NOT be represented as including PostgreSQL; replay after database commit MAY repeat the database write, but MUST NOT create another database row for the same `event_id`.

#### Scenario: Successful processing
- **GIVEN** the source topic contains a valid cut and PostgreSQL is available
- **WHEN** the first consumer writes that cut to its table and commits its Kafka transaction
- **THEN** the table contains one row for the cut ID, a committed-read observer sees one processed Kafka record for that source record, and the consumer position advances atomically with the Kafka output

#### Scenario: Abort after database write before Kafka commit
- **GIVEN** the first consumer has committed the cut to PostgreSQL but has not committed its Kafka output and source offset
- **WHEN** its Kafka transaction aborts or the consumer crashes and then restarts
- **THEN** the attempted Kafka output and source offset are not committed, reprocessing eventually produces one committed output record for that source record, and the database table retains exactly one row for that `event_id`

### Requirement: Separate PostgreSQL persistence guarantees
The system SHALL provide a PostgreSQL instance reachable from Compose services and from host localhost, with two independently inspectable cut-domain tables that have identical columns, data types, defaults, primary/unique constraints and update behavior except for table name. Each table SHALL use `event_id` as its primary key, with a JSONB payload and `updated_at` timestamp; each consumer SHALL upsert validated cuts on that key, replacing stored values on a conflicting later delivery while keeping exactly one row per ID. Each consumer SHALL write directly from its own source topic to its own table; there SHALL be no separate processed-topic database sink. The first consumer SHALL commit its database upsert before its Kafka output/offset transaction, and the second SHALL commit its own source offset only after its upsert succeeds. Repeated setup on a retained database MUST reconcile existing duplicate transactional IDs deterministically without deleting unrelated data or resetting volumes. Neither pipeline SHALL claim a distributed Kafka/PostgreSQL atomic commit.

#### Scenario: Inspect side-by-side tables
- **GIVEN** both cut sources have emitted the same indexed series and the consumers have caught up
- **WHEN** an operator inspects both PostgreSQL table definitions and queries rows by `event_id`
- **THEN** columns, types, defaults, and primary/unique constraints match, and each table contains at most one row for each valid cut ID

#### Scenario: Replay after database commit before Kafka offset commit
- **GIVEN** either consumer has committed a cut to PostgreSQL but has not committed its Kafka source offset
- **WHEN** that consumer restarts and reads the same cut again
- **THEN** its table retains exactly one row for that `event_id`, the source offset advances only after the required Kafka or database operation succeeds, and no cross-system atomicity is claimed

#### Scenario: Upsert a changed cut with the same ID
- **GIVEN** either consumer has stored a valid cut with a particular `event_id`
- **WHEN** it consumes a later valid cut with the same `event_id` but different values
- **THEN** its table contains exactly one row for that ID with the later cut's values, and its source offset advances only after the upsert succeeds

#### Scenario: Upgrade a retained database
- **GIVEN** the old transactional cut table contains repeated `event_id` rows and the idempotent table contains retained cuts
- **WHEN** the schema upgrade runs more than once without resetting volumes
- **THEN** one deterministic latest row per `event_id` survives in the transactional table, both tables have matching definitions, and other retained records remain available

#### Scenario: Database unavailable
- **GIVEN** PostgreSQL rejects a write for either consumer
- **WHEN** that consumer processes the cut
- **THEN** it does not advance its source-group position past that cut; the first consumer does not commit Kafka output/offset, and processing retries when PostgreSQL becomes available

### Requirement: Cut pipeline monitoring on the existing dashboard
The system SHALL add live, distinguishable cut-pipeline panels to the existing localhost-accessible, provisioned Kafka/Grafana dashboard, using the existing Prometheus collection service rather than a separate dashboard or monitoring stack. Both cut producers MUST have separate throughput and error-rate time-series panels; each of the two consumer groups (the transactional consumer and the idempotent consumer) MUST have separate lag, processing-throughput, and processing-error-rate time-series panels. Dashboard selectors MUST filter producer panels by producer client ID and consumer panels by consumer group ID, default to all available values, and apply the selected value to every related panel. A topic selector MUST filter topic-labeled lag series. The view MUST retain existing broker and registry panels and distinguish a caught-up consumer or stopped producer from missing or unavailable telemetry without manual dashboard import. It MUST use separate, readable legends, titles, and units for metrics with different meanings.

#### Scenario: Observe active cut pipelines
- **GIVEN** the default stack is running and metrics have been collected while producers remain active past their former finite run
- **WHEN** an operator opens the existing provisioned Kafka dashboard on localhost
- **THEN** broker and registry panels remain available, both producers have separately labeled ongoing throughput and error-rate panels, and both consumer groups have separately labeled lag, throughput, and error-rate panels

#### Scenario: Filter cut pipeline series
- **GIVEN** cut producer and consumer metrics are available for both pipeline identities
- **WHEN** the dashboard opens with its selectors set to all and an operator selects one producer client ID or one consumer group ID
- **THEN** all matching producer or consumer series are visible by default and only the selected identity's series appear in its related panels after filtering

#### Scenario: Observe idle and failed components
- **GIVEN** a producer has stopped, a consumer group has caught up, or a metrics target is unavailable
- **WHEN** an operator inspects the dashboard
- **THEN** stopped/idle throughput and errors are distinguishable from absent telemetry, and caught-up group lag is distinguishable from missing lag data

#### Scenario: Observe processing failure
- **GIVEN** a cut producer send or consumer processing/database write fails, or a metrics target becomes unreachable
- **WHEN** monitoring updates
- **THEN** the affected producer or consumer group has an observable error-rate indicator or unavailable-target indication, rather than appearing healthy and idle
