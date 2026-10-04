# Proposal

## Why

The current dashboards hardcode producer and consumer identities and combine different signals into overloaded panels, making it difficult to compare pipeline behavior or quickly identify component health. Separating related metrics, adding operational selectors, and using visualizations suited to rates, lag, counts, and health will make the two provisioned dashboards easier to scan while preserving their comparison purpose.

## What Changes

- Replace combined producer and consumer panels with separate throughput, error-rate, and consumer-lag panels.
- Add dashboard selectors for producer client IDs, consumer group IDs, and topics where the selected metric exposes a topic dimension; selectors default to all available values. Producer throughput and error panels use client IDs; consumer throughput, error, and lag panels use consumer groups.
- Use time-series visualizations for rates and lag, counter/stat visualizations for DLQ depth and unreconciled count, and colored component-health panels with green for healthy and red for unhealthy. Keep unavailable telemetry distinguishable from a healthy zero.
- Reorganize panels into clear operational sections and use consistent titles, units, descriptions, legends, and sizing based on Grafana dashboard guidance.
- Keep every shared panel identical between the base and improved dashboards. Keep the DLQ, unreconciled-count, and staleness panels exclusive to the improved dashboard.
- Extend dashboard contract tests and the dashboard derivation workflow to protect selectors, panel types, health states, and shared-panel parity.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `base-streaming-platform`: Define the structured, filterable dashboard layout, appropriate visualizations, and per-component health presentation.
- `exactly-once-semantics`: Define separate, selectable cut producer and consumer metric panels instead of combined hardcoded panels.
- `metrics-that-matter`: Preserve exact parity for shared panels while retaining outcome panels only on the improved dashboard, with count and health visualizations.

## Impact

Affected areas include the provisioned Grafana dashboard JSON files, dashboard derivation script, Prometheus/JMX metric labels or queries if required to support filtering, monitoring contract tests, and concise dashboard documentation. No Kafka processing guarantees, topic contents, database behavior, or synthetic data semantics change. Grafana's official dashboard guidance recommends reusable variables, clear panel descriptions, and visualizations appropriate to the data; use time series for changing values over time and stat-style panels for single-value status/counts (https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/best-practices/ and https://grafana.com/docs/grafana/latest/panels/visualizations/time-series/).

The change is working when the dashboard contracts pass, each selector defaults to all available values and narrows the intended panels, health colors reflect target availability without treating missing data as healthy, and normalized shared panels remain identical across both dashboards while improved-only outcome panels remain exclusive. To roll back, restore the prior dashboard JSON and derivation/test expectations; retain the existing telemetry exporters and persisted Kafka/PostgreSQL state.
