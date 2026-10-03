import argparse
import copy
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASE_DASHBOARD_PATH = ROOT / "monitoring" / "grafana" / "dashboards" / "base-streaming-platform.json"
DASHBOARD_PATH = ROOT / "monitoring" / "grafana" / "dashboards" / "metrics-that-matter.json"
OUTCOME_PANELS = [
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Retained DLQ Kafka records, including duplicates. The count decreases when Kafka retention removes records.",
        "fieldConfig": {"defaults": {"noValue": "No data", "unit": "short"}, "overrides": []},
        "gridPos": {"h": 6, "w": 8, "x": 0, "y": 42},
        "id": 14,
        "targets": [
            {"expr": "mill_tool_events_dlq_topic_length", "legendFormat": "retained records", "refId": "A"},
            {"expr": "mill_tool_events_dlq_collection_success", "legendFormat": "Kafka collection healthy", "refId": "B"},
        ],
        "title": "Dead-letter topic length",
        "type": "timeseries",
    },
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Database rows whose processed flag is false. This is independent of the Kafka DLQ topic length.",
        "fieldConfig": {"defaults": {"noValue": "No data", "unit": "short"}, "overrides": []},
        "gridPos": {"h": 6, "w": 8, "x": 8, "y": 42},
        "id": 15,
        "targets": [
            {"expr": "mill_tool_events_unreconciled", "legendFormat": "unprocessed rows", "refId": "A"},
            {"expr": "mill_tool_events_outcome_collection_success", "legendFormat": "PostgreSQL collection healthy", "refId": "B"},
        ],
        "title": "Unreconciled event rows",
        "type": "timeseries",
    },
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Mean age in seconds of the latest successfully updated row for each unique tool instance.",
        "fieldConfig": {"defaults": {"noValue": "No data", "unit": "s"}, "overrides": []},
        "gridPos": {"h": 6, "w": 8, "x": 16, "y": 42},
        "id": 16,
        "targets": [
            {"expr": "mill_tool_events_average_staleness_seconds", "legendFormat": "average staleness", "refId": "A"},
            {"expr": "mill_tool_events_outcome_collection_success", "legendFormat": "PostgreSQL collection healthy", "refId": "B"},
        ],
        "title": "Average entity staleness",
        "type": "timeseries",
    },
]


def derive_dashboard(baseline):
    dashboard = copy.deepcopy(baseline)
    dashboard["title"] = "Metrics That Matter"
    dashboard["uid"] = "metrics-that-matter"
    dashboard["panels"].extend(copy.deepcopy(OUTCOME_PANELS))
    return dashboard


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    baseline = json.loads(BASE_DASHBOARD_PATH.read_text())
    derived = derive_dashboard(baseline)
    serialized = json.dumps(derived, indent=2) + "\n"

    if args.check:
        if not DASHBOARD_PATH.is_file() or json.loads(DASHBOARD_PATH.read_text()) != derived:
            raise SystemExit(f"Metrics-that-matter dashboard is stale; regenerate {DASHBOARD_PATH}")
        return

    DASHBOARD_PATH.write_text(serialized)


if __name__ == "__main__":
    main()
