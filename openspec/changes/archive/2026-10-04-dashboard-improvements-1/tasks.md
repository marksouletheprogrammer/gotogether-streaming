# Tasks

## 1. Dashboard selectors and separate pipeline panels

- [x] 1.1 Add monitoring contract tests for producer-client, consumer-group, and topic selectors, including All as the default and selector use on the appropriate panel queries (scenarios: Filter producer and consumer metrics; Filter cut pipeline series; Dashboard filters and shared-panel parity); verify the focused tests fail against the current dashboard.
- [x] 1.2 Add contract assertions for separate producer throughput/error and consumer throughput/error/lag panels, correct units, and the existing `client_id`, `group_id`, and topic labels (scenarios: Open an active dashboard; Observe active cut pipelines); verify the new assertions fail before changing the dashboard.
- [x] 1.3 Define the Grafana variables with All defaults and split the base dashboard's combined producer and consumer panels into independently titled time-series panels using the existing telemetry labels (scenarios: Filter producer and consumer metrics; Filter cut pipeline series); verify selector and panel contract tests pass.
- [x] 1.4 Refactor related panel titles, legends, rate windows, and units for consistent scanning without changing the tested selector behavior (scenarios: Observe active cut pipelines; Open an active dashboard); verify the monitoring contract suite remains green.

## 2. Component health and dashboard layout

- [x] 2.1 Add tests for one health panel per monitored component, green/healthy and red/unhealthy states, and a distinct unavailable/no-data state (scenarios: Broker or registry becomes unreachable; Observe idle, caught-up, and missing telemetry; Observe idle and failed components); verify the focused assertions fail before panel changes.
- [x] 2.2 Add individually labeled component-health panels with explicit value mappings so target failure is not presented as healthy and missing telemetry is not presented as zero (scenarios: Broker or registry becomes unreachable; Observe idle, caught-up, and missing telemetry); verify the health-state contract tests pass.
- [x] 2.3 Reorganize the base dashboard into broker/registry, producer, consumer, and health sections with consistent panel sizing and descriptions (scenarios: Open an active dashboard; Observe active cut pipelines); verify all dashboard contract tests pass.

## 3. Improved dashboard outcomes and parity

- [x] 3.1 Extend dashboard derivation tests to require identical shared panels and variables, only three improved-dashboard outcome panels, stat/counter visualization for DLQ length and unreconciled count, and a seconds-based staleness time series (scenarios: Compare dashboards; Inspect separate DLQ and reconciliation counts; Observe staleness and unavailable telemetry); verify the new assertions fail before updating derivation output.
- [x] 3.2 Update the outcome panel definitions to use stat/counter visualization for DLQ length and unreconciled count while preserving source-health/no-data behavior and the staleness time series; regenerate the improved dashboard from the complete base dashboard (scenarios: Inspect separate DLQ and reconciliation counts; Kafka DLQ metrics unavailable; Compare dashboards); verify the derivation equality check passes.
- [x] 3.3 Refactor the derivation and dashboard contract checks to make the shared-panel and shared-variable source-of-truth explicit without changing panel behavior (scenarios: Compare dashboards; Dashboard filters and shared-panel parity); verify the improved dashboard remains an exact derivation plus the three exclusive outcome panels.

## 4. Documentation and verification

- [x] 4.1 Document the dashboard selectors, All defaults, panel groupings, health-state colors, and the distinct improved-dashboard outcome panels in the README (scenario: Open an active dashboard); verify the documented names and behavior match the provisioned dashboard JSON.
- [x] 4.2 Run `python -m unittest discover -s tests -v` and `python scripts/derive_metrics_that_matter_dashboard.py --check`; verify the full monitoring contract and regenerated dashboard checks pass without Docker or destructive reset.
