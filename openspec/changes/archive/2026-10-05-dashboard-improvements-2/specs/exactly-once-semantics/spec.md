# Spec Delta

## MODIFIED Requirements

### Requirement: Cut pipeline monitoring on the existing dashboard
The system SHALL provide live, distinguishable cut-pipeline panels across the existing localhost-accessible, provisioned Kafka/Grafana dashboards, using the existing Prometheus collection service rather than a separate monitoring stack. Both cut producers MUST have separate throughput and error-rate time-series panels on both dashboards; each of the two consumer groups (the transactional consumer and the idempotent consumer) MUST have separate lag and processing-throughput time-series panels on both dashboards, with their processing-error-rate time series exclusively on CNC Event Outcomes (Improved). Dashboard selectors MUST filter producer panels by producer client ID and consumer panels by consumer group ID, default to all available values, and apply the selected value to every related panel on each dashboard. A topic selector MUST filter topic-labeled lag series. Both views MUST retain existing broker and registry panels and distinguish an idle or caught-up component from missing or unavailable telemetry without manual dashboard import. They MUST use separate, readable legends, titles, and units for metrics with different meanings.

#### Scenario: Observe active cut pipelines
- **GIVEN** the default stack is running and metrics have been collected from both cut pipelines
- **WHEN** an operator opens the two provisioned dashboards on localhost
- **THEN** broker and registry panels remain available in both, both producers have separately labeled throughput and error-rate panels in both, and both consumer groups have separately labeled lag and throughput panels in both and processing-error-rate series only in the improved view

#### Scenario: Filter cut pipeline series
- **GIVEN** cut producer and consumer metrics are available for both pipeline identities
- **WHEN** either dashboard opens with its selectors set to all and an operator selects one producer client ID or one consumer group ID
- **THEN** all matching producer or consumer series are visible by default and only the selected identity's series appear in related panels, including processing errors on the improved dashboard

#### Scenario: Observe idle and failed components
- **GIVEN** a producer is idle or a consumer group has caught up, and monitoring targets remain healthy
- **WHEN** an operator inspects either dashboard
- **THEN** idle throughput and errors are distinguishable from absent telemetry, and caught-up group lag is distinguishable from missing lag data

#### Scenario: Observe processing failure
- **GIVEN** a cut producer send or consumer processing/database write fails, or a metrics target becomes unreachable
- **WHEN** monitoring updates
- **THEN** a producer send failure has an observable error-rate indicator on both views, a consumer processing failure has an error-rate indicator on the improved view, and an unavailable target does not appear healthy and idle
