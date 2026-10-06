import argparse
import copy
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASE_DASHBOARD_PATH = ROOT / "monitoring" / "grafana" / "dashboards" / "base-streaming-platform.json"
DASHBOARD_PATH = ROOT / "monitoring" / "grafana" / "dashboards" / "metrics-that-matter.json"

CONSUMER_PROCESSING_ERROR_PANEL = {
    "datasource": {"type": "prometheus", "uid": "prometheus"},
    "description": "Five-minute consumer processing-error rate by consumer group ID. A zero rate is healthy only when the target is available; No data means telemetry is unavailable.",
    "fieldConfig": {"defaults": {"noValue": "No data", "unit": "ops"}, "overrides": []},
    "gridPos": {"h": 6, "w": 8, "x": 16, "y": 21},
    "id": 11,
    "targets": [
        {"expr": "sum by (group_id) (rate(mill_cuts_consumer_processing_errors_total{group_id=~\"${consumer_group_id:regex}\"}[5m]))", "legendFormat": "cut {{group_id}}", "refId": "A"},
        {"expr": "sum by (group_id) (rate(mill_tool_events_consumer_processing_errors_total{group_id=~\"${consumer_group_id:regex}\"}[5m]))", "legendFormat": "event {{group_id}}", "refId": "B"}
    ],
    "title": "Consumer processing errors",
    "type": "timeseries"
}

OUTCOME_PANELS = [
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Retained DLQ Kafka records, including duplicates. A healthy zero is shown as zero; unavailable Kafka collection is shown separately.",
        "fieldConfig": {
            "defaults": {"noValue": "No data", "unit": "short"},
            "overrides": [],
        },
        "gridPos": {"h": 6, "w": 8, "x": 0},
        "id": 27,
        "options": {"colorMode": "value", "graphMode": "none", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "auto"},
        "targets": [
            {"expr": "mill_tool_events_dlq_topic_length", "legendFormat": "retained records", "refId": "A"},
        ],
        "title": "Dead-letter topic length",
        "type": "stat",
    },
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Database rows whose processed flag is false. This is independent of the Kafka DLQ topic length; a healthy zero is distinct from unavailable PostgreSQL collection.",
        "fieldConfig": {
            "defaults": {"noValue": "No data", "unit": "short"},
            "overrides": [],
        },
        "gridPos": {"h": 6, "w": 8, "x": 8},
        "id": 28,
        "options": {"colorMode": "value", "graphMode": "none", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "auto"},
        "targets": [
            {"expr": "mill_tool_events_unreconciled", "legendFormat": "unprocessed rows", "refId": "A"},
        ],
        "title": "Unreconciled event rows",
        "type": "stat",
    },
    {
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "description": "Mean age in seconds of the latest successfully updated row for each unique tool instance. Missing rows or failed database collection remain unavailable.",
        "fieldConfig": {
            "defaults": {"noValue": "No data", "unit": "s"},
            "overrides": [],
        },
        "gridPos": {"h": 6, "w": 8, "x": 16},
        "id": 29,
        "targets": [
            {"expr": "mill_tool_events_average_staleness_seconds", "legendFormat": "average staleness", "refId": "A"},
        ],
        "title": "Average entity staleness",
        "type": "timeseries",
    },
]

EVENT_OUTCOMES_ROW = {
    "collapsed": False,
    "gridPos": {"h": 1, "w": 24, "x": 0},
    "id": 30,
    "panels": [],
    "title": "Event outcomes",
    "type": "row",
}


def derive_dashboard(base_dashboard):
    dashboard = copy.deepcopy(base_dashboard)
    dashboard["title"] = "CNC Event Outcomes (Improved)"
    dashboard["uid"] = "metrics-that-matter"
    
    # Find the Component health row
    health_row_index = None
    for i, panel in enumerate(dashboard["panels"]):
        if panel.get("type") == "row" and panel.get("title") == "Component health":
            health_row_index = i
            break
    
    if health_row_index is None:
        raise ValueError("Component health row not found in base dashboard")
    
    # Get the original y position of the Component health row
    original_health_row_y = dashboard["panels"][health_row_index]["gridPos"]["y"]
    
    # Insert consumer processing error panel before Component health row (at same y as consumer panels)
    error_panel = copy.deepcopy(CONSUMER_PROCESSING_ERROR_PANEL)
    error_panel["gridPos"]["y"] = original_health_row_y  # Same y as consumer throughput/lag
    dashboard["panels"].insert(health_row_index, error_panel)
    health_row_index += 1  # Adjust index after insertion
    
    # Insert Event outcomes row (after error panel, which ends at original_health_row_y + 6)
    outcomes_row = copy.deepcopy(EVENT_OUTCOMES_ROW)
    outcomes_row["gridPos"]["y"] = original_health_row_y + 6
    dashboard["panels"].insert(health_row_index, outcomes_row)
    health_row_index += 1  # Adjust index after insertion
    
    # Insert outcome panels after the Event outcomes row
    outcome_y = original_health_row_y + 7  # 6 for error panel + 1 for row
    for panel in copy.deepcopy(OUTCOME_PANELS):
        panel["gridPos"]["y"] = outcome_y
        dashboard["panels"].insert(health_row_index, panel)
        health_row_index += 1
    
    # Update Component health row position (after outcome panels)
    new_health_row_y = outcome_y + 6
    health_row = dashboard["panels"][health_row_index]
    health_row["gridPos"]["y"] = new_health_row_y
    
    # Calculate the shift for health panels
    health_shift = new_health_row_y - original_health_row_y
    
    # Shift all health panels down
    for i in range(health_row_index + 1, len(dashboard["panels"])):
        panel = dashboard["panels"][i]
        if "gridPos" in panel:
            panel["gridPos"]["y"] += health_shift
    
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
