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


if __name__ == "__main__":
    unittest.main()
