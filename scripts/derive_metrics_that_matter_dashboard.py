import argparse
import copy
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASE_DASHBOARD_PATH = ROOT / "monitoring" / "grafana" / "dashboards" / "base-streaming-platform.json"
DASHBOARD_PATH = ROOT / "monitoring" / "grafana" / "dashboards" / "metrics-that-matter.json"
HEALTH_VALUE_MAPPING = {
    "options": {
        "0": {"color": "red", "index": 1, "text": "Unavailable"},
        "1": {"color": "green", "index": 0, "text": "Healthy"},
    },
    "type": "value",
}
OUTCOME_PANELS = [
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Retained DLQ Kafka records, including duplicates. A healthy zero is shown as zero; unavailable Kafka collection is shown separately.",
        "fieldConfig": {
            "defaults": {"noValue": "No data", "unit": "short"},
            "overrides": [{"matcher": {"id": "byName", "options": "Kafka collection healthy"}, "properties": [{"id": "mappings", "value": [HEALTH_VALUE_MAPPING]}]}],
        },
        "gridPos": {"h": 6, "w": 8, "x": 0},
        "id": 27,
        "options": {"colorMode": "value", "graphMode": "none", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "auto"},
        "targets": [
            {"expr": "mill_tool_events_dlq_topic_length", "legendFormat": "retained records", "refId": "A"},
            {"expr": "mill_tool_events_dlq_collection_success", "legendFormat": "Kafka collection healthy", "refId": "B"},
        ],
        "title": "Dead-letter topic length",
        "type": "stat",
    },
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Database rows whose processed flag is false. This is independent of the Kafka DLQ topic length; a healthy zero is distinct from unavailable PostgreSQL collection.",
        "fieldConfig": {
            "defaults": {"noValue": "No data", "unit": "short"},
            "overrides": [{"matcher": {"id": "byName", "options": "PostgreSQL collection healthy"}, "properties": [{"id": "mappings", "value": [HEALTH_VALUE_MAPPING]}]}],
        },
        "gridPos": {"h": 6, "w": 8, "x": 8},
        "id": 28,
        "options": {"colorMode": "value", "graphMode": "none", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "auto"},
        "targets": [
            {"expr": "mill_tool_events_unreconciled", "legendFormat": "unprocessed rows", "refId": "A"},
            {"expr": "mill_tool_events_outcome_collection_success", "legendFormat": "PostgreSQL collection healthy", "refId": "B"},
        ],
        "title": "Unreconciled event rows",
        "type": "stat",
    },
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Mean age in seconds of the latest successfully updated row for each unique tool instance. Missing rows or failed database collection remain unavailable.",
        "fieldConfig": {
            "defaults": {"noValue": "No data", "unit": "s"},
            "overrides": [{"matcher": {"id": "byName", "options": "PostgreSQL collection healthy"}, "properties": [{"id": "mappings", "value": [HEALTH_VALUE_MAPPING]}, {"id": "unit", "value": "short"}]}],
        },
        "gridPos": {"h": 6, "w": 8, "x": 16},
        "id": 29,
        "targets": [
            {"expr": "mill_tool_events_average_staleness_seconds", "legendFormat": "average staleness", "refId": "A"},
            {"expr": "mill_tool_events_outcome_collection_success", "legendFormat": "PostgreSQL collection healthy", "refId": "B"},
        ],
        "title": "Average entity staleness",
        "type": "timeseries",
    },
]


def derive_dashboard(base_dashboard):
    dashboard = copy.deepcopy(base_dashboard)
    dashboard["title"] = "Metrics That Matter"
    dashboard["uid"] = "metrics-that-matter"
    outcome_y = max(panel["gridPos"]["y"] + panel["gridPos"]["h"] for panel in dashboard["panels"])
    for panel in copy.deepcopy(OUTCOME_PANELS):
        panel["gridPos"]["y"] = outcome_y
        dashboard["panels"].append(panel)
    return dashboard


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    base_dashboard = json.loads(BASE_DASHBOARD_PATH.read_text())
    derived = derive_dashboard(base_dashboard)
    serialized = json.dumps(derived, indent=2) + "\n"

    if args.check:
        if not DASHBOARD_PATH.is_file() or json.loads(DASHBOARD_PATH.read_text()) != derived:
            raise SystemExit(f"Metrics-that-matter dashboard is stale; regenerate {DASHBOARD_PATH}")
        return

    DASHBOARD_PATH.write_text(serialized)


if __name__ == "__main__":
    main()
