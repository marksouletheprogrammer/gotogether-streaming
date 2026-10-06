# Design

## Context

See proposal.md for motivation and the three capability deltas for behavior. Grafana file provisioning currently loads `base-streaming-platform.json` and the generated `metrics-that-matter.json`. `scripts/derive_metrics_that_matter_dashboard.py` deep-copies the base dashboard and appends three outcome panels after its ten component-health panels; `--check` compares the generated dashboard with the checked-in JSON. The base JSON has one processing-error panel (id 11) between throughput and lag in the consumer row. The outcome definitions each include a separate collection-success target and field override. Tests currently demand exact base-prefix parity, append-only outcomes, all JMX counters on the base, and unchanged Grafana files in the StreamLens contract. README documents both old names and old placement. Do not change `DATA.md`, exporters, metrics, or Compose lifecycle.

## Goals / Non-Goals

**Goals:** Preserve stable dashboard UIDs and provisioning paths while making the improved view a deliberate superset of the operational view's data panels (with the one improved-only processing-error panel), keeping shared queries, visualization settings, IDs, and selectors identical apart from grid positions required by reflow. Make layout deterministic and outcome values readable without secondary health series.

**Non-Goals:** Rename dashboard files, UIDs, Prometheus metrics, or Grafana folders; replace health panels, alter Kafka/PostgreSQL processing, or add a separate visualization stack.

## Decisions

1. **Derive the improved view from the operational JSON, with one explicit extra error panel.** Remove the processing-error panel from the base JSON and compact its consumer row so lag occupies the vacant slot. Define the unchanged error panel (including both cut and event queries, units, description, and group selector) in the derivation script, inserting it into the improved consumer row before lag; move lag back to its prior x-position in the derived view. Keep base/derived UIDs and filenames unchanged; change only displayed titles to `CNC Streaming Operations` and `CNC Event Outcomes (Improved)`. Alternative: duplicate the whole improved JSON manually; rejected because it loses the reproducible shared-panel contract. Alternative: rename UIDs/files to match titles; rejected because existing bookmarks and smoke checks rely on stable identities.

2. **Insert outcomes before health and reflow only the improved dashboard.** After the consumer row, insert an expanded, dedicated `Event outcomes` row (unique row id), place the three outcome data panels across one six-unit-high grid row, then shift the existing Component health row and every following health panel down by the new row and outcome height. The base dashboard keeps its four rows and original component-health positioning; the improved dashboard gains one row and retains every health panel. Use panel IDs to locate insertion and avoid relying on the previous append-only bottom-of-dashboard calculation. Alternative: append outcomes below health with just a visual label; rejected because it does not place them above Component health.

3. **Keep a single value series per outcome.** Remove each outcome's collection-success target and associated `fieldConfig` overrides/mappings; retain its original metric target, panel type, units, and `noValue` behavior. Update descriptions to explain missing measurements versus healthy zero without promising an in-panel health indication. Existing dedicated component-health panels remain untouched; query results absent due to failed collection show no data instead of zero. Alternative: hide the health series without removing it; rejected because it still embeds health data in the value panel and can affect legend/reduction behavior.

4. **Change tests and runbook together.** Adapt `tests/test_metrics_dashboard_derivation.py` to assert shared panel definitions match after excluding `gridPos` rather than a base prefix, the single derived-only error panel, IDs/row order/grid positions, and single-series outcome queries. Adapt `tests/test_monitoring_contract.py` to check base metrics except consumer-processing error, verify its presence/filtering on the derived dashboard, and keep exporter-label coverage. Its StreamLens-specific assertion that dashboard files must be git-clean should be replaced by focused StreamLens/non-Grafana invariants so this authorized dashboard-only change does not fail unrelated contracts. Keep smoke checks keyed by stable UID; update README titles, outcome row order, and no-data explanation. Alternative: loosen all parity/monitoring assertions; rejected because shared-panel drift and loss of outcome telemetry would go undetected.

## Risks / Trade-offs

- [Existing main specs and tests mandate base consumer errors and exact shared-panel parity] → Update all three relevant capability deltas and replace only those assertions with explicit shared-versus-improved-only checks.
- [Grid reflow may overlap component panels or duplicate panel IDs] → Check unique IDs, row order, panel bounding boxes, and base/derived differences in focused JSON contract tests and inspect provisioned views during authorized runtime verification.
- [Removing secondary health series loses per-panel collection-status text] → Preserve `No data` rather than injecting zero, retain independent component-health panels and metric-source documentation; verify failed-source versus healthy-zero behavior.

## Migration Plan

Update focused tests first, then base JSON and derivation, regenerate the improved JSON, and update README. Run Python dashboard tests and the derivation `--check`; if a running Grafana instance is available and Docker use is approved, confirm provisioned titles and placement without resetting volumes. Roll back by restoring the previous dashboard JSON, derivation code, tests, and README; unchanged UIDs, metric names, and persistent state require no data migration.
