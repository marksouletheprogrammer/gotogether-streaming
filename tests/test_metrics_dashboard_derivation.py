import json
import unittest
from pathlib import Path

from scripts.derive_metrics_that_matter_dashboard import derive_dashboard


ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = ROOT / "monitoring" / "grafana" / "dashboards"


class MetricsDashboardDerivationTests(unittest.TestCase):
    def test_outcome_dashboard_is_derived_from_the_current_baseline(self):
        baseline = json.loads((DASHBOARDS / "base-streaming-platform.json").read_text())
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())

        self.assertEqual(actual, derive_dashboard(baseline))

    def test_shared_panels_and_variables_match_and_outcomes_use_suitable_visualizations(self):
        baseline = json.loads((DASHBOARDS / "base-streaming-platform.json").read_text())
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())
        outcomes_by_title = {panel["title"]: panel for panel in actual["panels"]}
        dlq = outcomes_by_title["Dead-letter topic length"]
        unreconciled = outcomes_by_title["Unreconciled event rows"]
        staleness = outcomes_by_title["Average entity staleness"]

        self.assertEqual(dlq["type"], "stat")
        self.assertEqual(unreconciled["type"], "stat")
        self.assertEqual(staleness["type"], "timeseries")
        self.assertEqual(staleness["fieldConfig"]["defaults"]["unit"], "s")
        self.assertEqual(staleness["fieldConfig"]["defaults"]["noValue"], "No data")
        self.assertIn("mill_tool_events_dlq_collection_success", {target["expr"] for target in dlq["targets"]})
        self.assertIn("mill_tool_events_outcome_collection_success", {target["expr"] for target in unreconciled["targets"]})
        self.assertIn("mill_tool_events_outcome_collection_success", {target["expr"] for target in staleness["targets"]})

        baseline_panels = baseline["panels"]
        self.assertEqual(actual["panels"][:len(baseline_panels)], baseline_panels)
        self.assertEqual(len(actual["panels"]), len(baseline_panels) + 3)
        self.assertEqual(actual["templating"]["list"], baseline["templating"]["list"])
        self.assertEqual(
            {panel["title"] for panel in actual["panels"][len(baseline_panels):]},
            {"Dead-letter topic length", "Unreconciled event rows", "Average entity staleness"},
        )
        bottom = max(panel["gridPos"]["y"] + panel["gridPos"]["h"] for panel in baseline_panels)
        self.assertEqual(
            [panel["gridPos"] for panel in actual["panels"][len(baseline_panels):]],
            [{"h": 6, "w": 8, "x": x, "y": bottom} for x in (0, 8, 16)],
        )


if __name__ == "__main__":
    unittest.main()
