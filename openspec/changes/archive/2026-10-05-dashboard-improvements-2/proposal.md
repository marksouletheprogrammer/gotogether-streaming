# Proposal

## Why

The two Grafana views currently duplicate consumer processing errors, bury the three event-outcome metrics after component health, and mix collection-health series into outcome panels. Their generic names also obscure the CNC streaming domain and which view is the improved one.

## What Changes

- Show the existing consumer processing-error time series only in the improved view; keep shared consumer throughput and lag panels in both views.
- Give the three event-outcome panels a dedicated row immediately before Component health in the improved view, rather than appending them below health.
- Remove collection-health targets, legends, and overrides from the three outcome panels; retain their primary Kafka/PostgreSQL metrics, units, no-data behavior, and separate component-health panels.
- Rename the visible dashboard titles to **CNC Streaming Operations** and **CNC Event Outcomes (Improved)**; retain existing dashboard UIDs and provisioned file paths so links and checks can continue to address them.
- Update the derivation, dashboard contracts, and dashboard-facing documentation to reflect deliberate panel differences and layout.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `base-streaming-platform`: The operational dashboard no longer displays consumer processing errors; it retains the other monitoring sections, filters, and component-health signals under a CNC-specific title.
- `exactly-once-semantics`: Cut consumer processing errors remain observable on the improved view instead of the operational dashboard, preserving the existing cut monitoring contract across both views.
- `metrics-that-matter`: The improved dashboard uniquely displays consumer processing errors and its three outcome panels in their own row above component health, without embedded collection-health series; shared-panel parity now excludes these four panels and titles are domain-specific.

## Impact

The provisioned Grafana JSON dashboards, `scripts/derive_metrics_that_matter_dashboard.py`, dashboard contract tests, and README dashboard guidance are affected. This is a deliberate change to the earlier shared-panel parity and base consumer-error requirements; no changes to exported metrics, Kafka/PostgreSQL behavior, dashboard UIDs, or monitoring infrastructure are intended.

The change is working when both dashboards are provisioned with their new titles, only the improved dashboard shows consumer processing errors and outcome panels, the three outcomes form a row directly above Component health without health-series targets, and existing metrics retain their correct value/no-data behavior. Roll back by restoring the previous dashboard JSON, derivation script, test expectations, and dashboard documentation; leave persistent data and exporters untouched.
