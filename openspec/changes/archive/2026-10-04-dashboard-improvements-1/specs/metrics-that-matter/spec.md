# Spec Delta

## MODIFIED Requirements

### Requirement: Two provisioned views of operational metrics
The existing Grafana dashboard SHALL retain its broker, registry, cut, and event metrics while presenting producer throughput, producer error rate, consumer throughput, consumer error rate, and consumer lag in separate time-series panels. Producer throughput/error panels SHALL be filterable by producer client ID; consumer throughput/error/lag panels SHALL be filterable by consumer group ID. The corresponding selectors MUST default to all available values and apply consistently to all related panels. Topic selection MUST filter panels whose metric series expose a topic label, including lag, without treating unlabeled producer metrics as topic-labeled. A second provisioned dashboard SHALL reproduce every shared panel and selector from the base dashboard with identical shared-panel definitions and only a distinct identity/title, plus exactly three outcome panels that remain exclusive to the second dashboard: DLQ topic length (sum across DLQ partitions of latest offset minus earliest retained offset), unreconciled count (reconciliation rows with `processed = false`), and average staleness in seconds (arithmetic mean of `now - updated_at` over the latest target row per unique `(machine_id, tool_instance_id)`). DLQ length SHALL be displayed as a single-value counter/stat, come from Kafka, count retained copies including duplicates, and vary with topic retention; it SHALL NOT be inferred from reconciliation rows. Unreconciled count SHALL be displayed as a single-value counter/stat, come from PostgreSQL, include rows for failed sends and DLQ events, and SHALL NOT be inferred from Kafka offsets. Average staleness SHALL remain a time-series value with seconds as its unit. On zero target rows staleness SHALL be shown as no data rather than a fabricated zero; zero Kafka topic length and zero unreconciled rows SHALL be shown as zero when their respective collection sources are healthy. A down Kafka or database metrics source MUST be distinguishable from a healthy zero reading without substituting one source's data for the other. Shared panel presentation, queries, and variables MUST stay identical across both dashboards as they evolve; the three outcome panels MUST exist only on the second dashboard.

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
