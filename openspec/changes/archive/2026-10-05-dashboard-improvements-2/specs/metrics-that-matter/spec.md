# Spec Delta

## MODIFIED Requirements

### Requirement: Two provisioned views of operational metrics
The operational Grafana dashboard SHALL retain its broker, registry, cut, and event metrics while presenting producer throughput, producer error rate, consumer throughput, and consumer lag in separate time-series panels, but SHALL NOT display consumer processing errors. The second dashboard, titled **CNC Event Outcomes (Improved)**, SHALL display the consumer processing-error time series exclusively, with its existing cut and event error metrics, units, and consumer-group filter. Producer throughput/error panels SHALL be filterable by producer client ID; consumer throughput/lag panels in both views and consumer processing errors in the improved view SHALL be filterable by consumer group ID. The corresponding selectors MUST default to all available values and apply consistently to all related panels. Topic selection MUST filter panels whose metric series expose a topic label, including lag, without treating unlabeled producer metrics as topic-labeled. Both dashboard UIDs SHALL remain stable. The improved dashboard SHALL reproduce every remaining shared panel and selector from the operational dashboard with identical shared-panel queries, visualization settings, IDs, and selectors (allowing grid positions to differ for reflow), plus exactly four improved-only data panels: consumer processing errors, DLQ topic length (sum across DLQ partitions of latest offset minus earliest retained offset), unreconciled count (reconciliation rows with `processed = false`), and average staleness in seconds (arithmetic mean of `now - updated_at` over the latest target row per unique `(machine_id, tool_instance_id)`). The three outcome panels SHALL be grouped under their own labeled row immediately above the Component health row; component-health panels SHALL remain in their own section. Each outcome panel SHALL contain only its own outcome metric, without a collection-health series, mapping, or legend; health signals SHALL remain available in dedicated component-health panels. DLQ length SHALL be displayed as a single-value counter/stat, come from Kafka, count retained copies including duplicates, and vary with topic retention; it SHALL NOT be inferred from reconciliation rows. Unreconciled count SHALL be displayed as a single-value counter/stat, come from PostgreSQL, include rows for failed sends and DLQ events, and SHALL NOT be inferred from Kafka offsets. Average staleness SHALL remain a time-series value with seconds as its unit. On zero target rows staleness SHALL be shown as no data rather than a fabricated zero; zero Kafka topic length and zero unreconciled rows SHALL be shown as zero when their respective collection sources are healthy. A down Kafka or database metrics source MUST be distinguishable from a healthy zero reading without substituting one source's data for the other.

#### Scenario: Compare dashboards
- **GIVEN** a live event pipeline and both dashboards provisioned on localhost
- **WHEN** an operator opens CNC Streaming Operations and CNC Event Outcomes (Improved)
- **THEN** each shows identical shared broker, registry, cut, and event panels and selectors, only the improved view shows consumer processing errors and the three outcome panels, and both dashboards retain their existing UIDs

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
- **THEN** its matching shared panels respond to the same selection rules, selectors default to all values, the improved-only consumer processing-error panel respects the consumer-group selector, and contract checks confirm that all remaining shared panels have identical definitions apart from grid positions while the four improved-only data panels remain exclusive to the second dashboard

#### Scenario: Outcome row and single-signal panels
- **GIVEN** both dashboards are provisioned
- **WHEN** an operator views the improved dashboard
- **THEN** the three outcome panels occupy a dedicated labeled row immediately before Component health, each outcome panel contains its value series only and no embedded collection-health display, and separate component-health panels remain available below the outcome row
