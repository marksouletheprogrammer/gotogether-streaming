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

    def test_improved_dashboard_titles_and_uids(self):
        baseline = json.loads((DASHBOARDS / "base-streaming-platform.json").read_text())
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())

        self.assertEqual(baseline["title"], "CNC Streaming Operations")
        self.assertEqual(baseline["uid"], "base-streaming-platform")
        self.assertEqual(actual["title"], "CNC Event Outcomes (Improved)")
        self.assertEqual(actual["uid"], "metrics-that-matter")

    def test_improved_dashboard_has_consumer_processing_error_panel(self):
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())
        panels_by_title = {panel["title"]: panel for panel in actual["panels"]}
        self.assertIn("Consumer processing errors", panels_by_title)
        error_panel = panels_by_title["Consumer processing errors"]
        self.assertEqual(error_panel["type"], "timeseries")
        expressions = {target["expr"] for target in error_panel["targets"]}
        self.assertIn("mill_cuts_consumer_processing_errors_total", " ".join(expressions))
        self.assertIn("mill_tool_events_consumer_processing_errors_total", " ".join(expressions))
        for expr in expressions:
            self.assertIn('group_id=~"${consumer_group_id:regex}"', expr)

    def test_shared_panels_match_except_gridpos_and_improved_only_panels(self):
        baseline = json.loads((DASHBOARDS / "base-streaming-platform.json").read_text())
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())

        baseline_panels = baseline["panels"]
        actual_panels = actual["panels"]

        # Build maps of panels by title for comparison (excluding improved-only panels)
        baseline_by_title = {p.get("title"): p for p in baseline_panels}
        actual_by_title = {p.get("title"): p for p in actual_panels}
        
        # Improved-only panels
        improved_only_titles = {
            "Consumer processing errors",
            "Event outcomes",
            "Dead-letter topic length",
            "Unreconciled event rows",
            "Average entity staleness",
        }
        
        # Verify shared panels match except for gridPos
        for title, baseline_panel in baseline_by_title.items():
            if title not in improved_only_titles:
                with self.subTest(panel=title):
                    self.assertIn(title, actual_by_title, f"Missing panel: {title}")
                    actual_panel = actual_by_title[title]
                    baseline_copy = json.loads(json.dumps(baseline_panel))
                    actual_copy = json.loads(json.dumps(actual_panel))
                    baseline_copy.pop("gridPos", None)
                    actual_copy.pop("gridPos", None)
                    self.assertEqual(actual_copy, baseline_copy, f"Panel {title} differs beyond gridPos")

        self.assertEqual(actual["templating"]["list"], baseline["templating"]["list"])

    def test_outcome_panels_have_single_target_and_no_collection_health_series(self):
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
        
        # Verify single outcome metric per panel, no collection-health series
        dlq_exprs = {target["expr"] for target in dlq["targets"]}
        self.assertIn("mill_tool_events_dlq_topic_length", dlq_exprs)
        self.assertNotIn("mill_tool_events_dlq_collection_success", dlq_exprs)
        
        unreconciled_exprs = {target["expr"] for target in unreconciled["targets"]}
        self.assertIn("mill_tool_events_unreconciled", unreconciled_exprs)
        self.assertNotIn("mill_tool_events_outcome_collection_success", unreconciled_exprs)
        
        staleness_exprs = {target["expr"] for target in staleness["targets"]}
        self.assertIn("mill_tool_events_average_staleness_seconds", staleness_exprs)
        self.assertNotIn("mill_tool_events_outcome_collection_success", staleness_exprs)

    def test_outcome_row_positioned_before_component_health(self):
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())
        panels = actual["panels"]
        
        # Find Component health row
        health_row_index = None
        for i, panel in enumerate(panels):
            if panel.get("type") == "row" and panel.get("title") == "Component health":
                health_row_index = i
                break
        
        self.assertIsNotNone(health_row_index, "Component health row not found")
        
        # Find outcome panels
        outcome_titles = {"Dead-letter topic length", "Unreconciled event rows", "Average entity staleness"}
        outcome_indices = [i for i, p in enumerate(panels) if p.get("title") in outcome_titles]
        
        self.assertEqual(len(outcome_indices), 3, "Should have exactly 3 outcome panels")
        
        # All outcome panels should come before Component health row
        for idx in outcome_indices:
            self.assertLess(idx, health_row_index, f"Outcome panel at {idx} should come before Component health at {health_row_index}")
        
        # Outcome panels should be contiguous
        outcome_indices.sort()
        for i in range(len(outcome_indices) - 1):
            self.assertEqual(outcome_indices[i+1] - outcome_indices[i], 1, "Outcome panels should be contiguous")

    def test_outcome_row_and_panels_have_unique_ids_and_valid_grid(self):
        actual = json.loads((DASHBOARDS / "metrics-that-matter.json").read_text())
        panels = actual["panels"]
        
        # Collect all panel IDs
        panel_ids = [p["id"] for p in panels if "id" in p]
        self.assertEqual(len(panel_ids), len(set(panel_ids)), "Panel IDs must be unique")
        
        # Check grid positions don't overlap
        for i, panel in enumerate(panels):
            if "gridPos" in panel:
                x, y, w, h = panel["gridPos"]["x"], panel["gridPos"]["y"], panel["gridPos"]["w"], panel["gridPos"]["h"]
                self.assertGreaterEqual(x, 0)
                self.assertGreaterEqual(y, 0)
                self.assertLessEqual(x + w, 24)
                for j, other in enumerate(panels):
                    if i != j and "gridPos" in other:
                        ox, oy, ow, oh = other["gridPos"]["x"], other["gridPos"]["y"], other["gridPos"]["w"], other["gridPos"]["h"]
                        # Check if they overlap
                        x_overlap = x < ox + ow and ox < x + w
                        y_overlap = y < oy + oh and oy < y + h
                        if x_overlap and y_overlap:
                            # Same row panels can overlap in y if they're in different x positions
                            if not (y == oy and x + w <= ox or ox + ow <= x):
                                self.fail(f"Panels {i} and {j} overlap: {panel.get('title')} and {other.get('title')}")


if __name__ == "__main__":
    unittest.main()
