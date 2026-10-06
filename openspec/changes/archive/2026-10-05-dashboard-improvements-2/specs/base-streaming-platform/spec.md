# Spec Delta

## MODIFIED Requirements

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
