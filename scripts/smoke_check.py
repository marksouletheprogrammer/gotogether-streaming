import json
import subprocess
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError


ROOT = Path(__file__).resolve().parents[1]
TOPIC = "cnc-demo-records"
SUBJECT = f"{TOPIC}-value"
REGISTRY_URL = "http://localhost:8081"
AKHQ_URL = "http://localhost:8080"
PROMETHEUS_URL = "http://localhost:9090"
GRAFANA_URL = "http://localhost:3000"
PROMETHEUS_JOBS = ("kafka-broker", "schema-registry")
REQUIRED_METRICS = (
    "kafka_server_broker_topic_metrics_bytes_in_total",
    "kafka_server_broker_topic_metrics_bytes_out_total",
    "kafka_server_under_replicated_partitions",
    "kafka_network_request_metrics_request_total_time_ms_mean",
    "kafka_schema_registry_jersey_request_rate",
    "kafka_schema_registry_jersey_request_error_rate",
)


def validate_topic_offsets(output):
    offsets = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.rsplit(":", 2)
        if len(parts) != 3 or parts[0] != TOPIC:
            raise ValueError(f"Unexpected topic offset row: {line}")
        try:
            partition, offset = int(parts[1]), int(parts[2])
        except ValueError as error:
            raise ValueError(f"Invalid topic offset row: {line}") from error
        if offset != 0:
            raise ValueError(f"{TOPIC} partition {partition} has nonzero offset {offset}")
        if partition in offsets:
            raise ValueError(f"Duplicate offset row for {TOPIC} partition {partition}")
        offsets[partition] = offset

    if not offsets:
        raise ValueError(f"{TOPIC} has no partition offsets; the topic may be missing")
    if set(offsets) != {0}:
        raise ValueError(f"{TOPIC} must have one partition (0); found {sorted(offsets)}")
    return offsets


def validate_registered_schema(subjects, version, canonical_schema):
    if SUBJECT not in subjects:
        raise ValueError(f"Schema Registry subject is missing: {SUBJECT}")
    if version.get("subject") != SUBJECT:
        raise ValueError(f"Latest schema response does not identify subject {SUBJECT}")
    if version.get("schemaType") != "JSON":
        raise ValueError(f"{SUBJECT} is not registered as a JSON Schema")

    try:
        registered_schema = json.loads(version["schema"])
    except (KeyError, json.JSONDecodeError) as error:
        raise ValueError(f"{SUBJECT} does not contain valid JSON Schema text") from error
    if registered_schema != canonical_schema:
        raise ValueError(f"Registered schema for {SUBJECT} does not match the canonical asset")
    return registered_schema


def validate_akhq_topic_names(topic_names):
    if TOPIC not in topic_names:
        raise ValueError(f"AKHQ cluster local does not list topic {TOPIC}")
    return topic_names


def validate_cluster_metadata(metadata, expected_host):
    brokers = metadata.get("brokers", [])
    if not metadata.get("cluster_id") or not brokers:
        raise ValueError("Kafka returned incomplete cluster metadata")
    if not any(broker.get("host") == expected_host for broker in brokers):
        raise ValueError(f"Kafka metadata did not advertise broker host {expected_host}: {brokers}")
    return metadata


def validate_prometheus_targets(active_targets):
    statuses = {}
    for job in PROMETHEUS_JOBS:
        targets = [target for target in active_targets if target.get("labels", {}).get("job") == job]
        if not targets:
            raise ValueError(f"Prometheus target is missing for job {job}")
        statuses[job] = all(target.get("health") == "up" for target in targets)
    return statuses


def validate_prometheus_metrics(results):
    missing = [name for name in REQUIRED_METRICS if not results.get(name)]
    if missing:
        raise ValueError(f"Prometheus has no live series for required metrics: {', '.join(missing)}")
    return results


def http_response(url):
    try:
        with urlopen(url, timeout=5) as response:
            return response.status, response.read()
    except (OSError, URLError) as error:
        raise RuntimeError(f"HTTP request failed for {url}: {error}") from error


def request_json(url):
    _, body = http_response(url)
    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Expected a JSON response from {url}: {error}") from error


def check_ready(url):
    status, _ = http_response(url)
    if not 200 <= status < 300:
        raise RuntimeError(f"Readiness check failed for {url}: HTTP {status}")


def prometheus_query(metric):
    response = request_json(f"{PROMETHEUS_URL}/api/v1/query?{urlencode({'query': metric})}")
    if response.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed for {metric}: {response}")
    return response.get("data", {}).get("result", [])


def fetch_cluster_metadata(bootstrap_server):
    try:
        client = KafkaAdminClient(
            bootstrap_servers=bootstrap_server,
            client_id="gotogether-smoke-check",
            request_timeout_ms=5000,
            bootstrap_timeout_ms=5000,
        )
        try:
            return client.describe_cluster()
        finally:
            client.close()
    except KafkaError as error:
        raise RuntimeError(f"Kafka metadata request failed for {bootstrap_server}: {error}") from error


def internal_cluster_metadata():
    command = [
        "docker", "compose", "exec", "-T", "-e", "KAFKA_OPTS=", "broker", "kafka-broker-api-versions",
        "--bootstrap-server", "broker:29092",
    ]
    result = subprocess.run(command, capture_output=True, text=True, cwd=ROOT)
    if result.returncode:
        raise RuntimeError(f"Internal Kafka metadata request failed: {result.stderr.strip()}")
    return result.stdout


def compose_services():
    result = subprocess.run(
        ["docker", "compose", "config", "--services"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if result.returncode:
        raise RuntimeError(f"Could not inspect Compose services: {result.stderr.strip()}")
    return result.stdout.splitlines()


def validate_service_scope(services):
    forbidden = [
        service for service in services
        if service in {"producer", "consumer", "database", "postgres"}
        or service.startswith(("producer-", "consumer-"))
    ]
    if forbidden:
        raise ValueError(f"Unexpected application services in base stack: {', '.join(forbidden)}")
    return services


def topic_offsets():

    command = [
        "docker", "compose", "exec", "-T", "-e", "KAFKA_OPTS=", "broker", "kafka-get-offsets",
        "--bootstrap-server", "broker:29092", "--topic", TOPIC, "--time", "-1",
    ]
    result = subprocess.run(command, capture_output=True, text=True, cwd=ROOT)
    if result.returncode:
        raise RuntimeError(f"Could not read {TOPIC} offsets: {result.stderr.strip()}")
    return result.stdout


def main():
    try:
        canonical_schema = json.loads((ROOT / "schemas" / "cnc-demo-records.schema.json").read_text())
        offsets = validate_topic_offsets(topic_offsets())
        validate_service_scope(compose_services())
        validate_cluster_metadata(fetch_cluster_metadata("127.0.0.1:9092"), "127.0.0.1")
        if not internal_cluster_metadata().strip():
            raise RuntimeError("Internal Kafka metadata request returned no broker data")
        validate_registered_schema(
            request_json(f"{REGISTRY_URL}/subjects"),
            request_json(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/latest"),
            canonical_schema,
        )
        validate_akhq_topic_names(request_json(f"{AKHQ_URL}/api/local/topic/name"))
        check_ready(f"{PROMETHEUS_URL}/-/ready")
        targets = request_json(f"{PROMETHEUS_URL}/api/v1/targets?state=active")
        if targets.get("status") != "success":
            raise RuntimeError(f"Prometheus target request failed: {targets}")
        target_status = validate_prometheus_targets(targets.get("data", {}).get("activeTargets", []))
        down_targets = [job for job, healthy in target_status.items() if not healthy]
        if down_targets:
            raise RuntimeError(f"Prometheus scrape targets are down: {', '.join(down_targets)}")
        validate_prometheus_metrics({metric: prometheus_query(metric) for metric in REQUIRED_METRICS})
        grafana_health = request_json(f"{GRAFANA_URL}/api/health")
        if grafana_health.get("database") != "ok":
            raise RuntimeError(f"Grafana is not healthy: {grafana_health}")
        dashboard = request_json(f"{GRAFANA_URL}/api/dashboards/uid/base-streaming-platform")
        if dashboard.get("dashboard", {}).get("uid") != "base-streaming-platform":
            raise RuntimeError("Grafana did not load the provisioned base streaming dashboard")
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Smoke check failed: {error}", file=sys.stderr)
        return 1

    print(f"Smoke check passed: {TOPIC} is empty, {SUBJECT} matches the canonical JSON Schema, AKHQ lists the topic, and broker/registry metrics and dashboards are ready across {len(offsets)} partition(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
