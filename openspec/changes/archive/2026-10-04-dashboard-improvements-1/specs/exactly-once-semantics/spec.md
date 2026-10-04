# Spec Delta

## MODIFIED Requirements

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
