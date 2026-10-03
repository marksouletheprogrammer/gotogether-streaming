import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError


ROOT = Path(__file__).resolve().parents[1]
TOPIC = "cnc-demo-records"
SUBJECT = f"{TOPIC}-value"
REGISTRY_URL = "http://localhost:8081"
AKHQ_URL = "http://localhost:8080"
PROMETHEUS_URL = "http://localhost:9090"
GRAFANA_URL = "http://localhost:3000"
PROMETHEUS_JOBS = (
    "kafka-broker",
    "schema-registry",
    "mill-cuts-transactional-producer",
    "mill-cuts-at-least-once-producer",
    "mill-cuts-transactional-consumer",
    "mill-cuts-idempotent-consumer",
    "mill-cuts-group-lag",
    "mill-tool-events-producer",
    "mill-tool-events-consumer",
    "mill-tool-events-outcomes",
)
CUT_TOPICS = (
    "mill-cuts-transactional-source",
    "mill-cuts-replay-source",
    "mill-cuts-committed",
)
EVENT_TOPICS = ("mill-tool-events-source", "mill-tool-events-dlq")
EVENT_SUBJECTS = tuple(f"{topic}-value" for topic in EVENT_TOPICS)
OUTCOME_DASHBOARD_UID = "metrics-that-matter"
REQUIRED_TOPICS = (TOPIC, *CUT_TOPICS, *EVENT_TOPICS)
CUT_SUBJECTS = tuple(f"{topic}-value" for topic in CUT_TOPICS)
RUNNING_SERVICES = (
    "broker",
    "schema-registry",
    "postgres",
    "akhq",
    "prometheus",
    "grafana",
    "mill-cuts-transactional-producer",
    "mill-cuts-at-least-once-producer",
    "mill-cuts-transactional-consumer",
    "mill-cuts-idempotent-consumer",
    "mill-cuts-lag-exporter",
    "mill-tool-events-producer",
    "mill-tool-events-consumer",
    "mill-tool-events-outcomes",
)
SETUP_SERVICES = (
    "topic-bootstrap",
    "cut-topic-bootstrap",
    "schema-bootstrap",
    "cuts-db-bootstrap",
    "cuts-avro-schema-bootstrap",
    "event-topic-bootstrap",
    "event-schema-bootstrap",
    "event-db-bootstrap",
    "cuts-app-build",
)
REQUIRED_METRICS = (
    "kafka_server_broker_topic_metrics_bytes_in_total",
    "kafka_server_broker_topic_metrics_bytes_out_total",
    "kafka_server_under_replicated_partitions",
    "kafka_network_request_metrics_request_total_time_ms_mean",
    "kafka_schema_registry_jersey_request_rate",
    "kafka_schema_registry_jersey_request_error_rate",
    "mill_cuts_producer_records_sent_total",
    "mill_cuts_producer_send_errors_total",
    "mill_cuts_consumer_records_processed_total",
    "mill_cuts_consumer_processing_errors_total",
    "mill_cuts_consumer_group_lag",
    "mill_cuts_consumer_lag_collection_success",
    "mill_tool_events_producer_records_sent_total",
    "mill_tool_events_producer_send_errors_total",
    "mill_tool_events_consumer_records_processed_total",
    "mill_tool_events_consumer_processing_errors_total",
    "mill_tool_events_dlq_topic_length",
    "mill_tool_events_dlq_collection_success",
    "mill_tool_events_unreconciled",
    "mill_tool_events_outcome_collection_success",
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
    return validate_topic_names(topic_names)


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


def compose_statuses():
    result = subprocess.run(
        ["docker", "compose", "ps", "--all", "--format", "json"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if result.returncode:
        raise RuntimeError(f"Could not inspect Compose service states: {result.stderr.strip()}")

    output = result.stdout.strip()
    if not output:
        return []
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        parsed = [json.loads(line) for line in output.splitlines() if line.strip()]
    return parsed if isinstance(parsed, list) else [parsed]


def validate_service_states(containers):
    by_service = {container.get("Service"): container for container in containers}
    missing = (set(RUNNING_SERVICES) | set(SETUP_SERVICES)) - set(by_service)
    if missing:
        raise ValueError(f"Compose containers are missing: {', '.join(sorted(missing))}")

    failures = []
    for service in RUNNING_SERVICES:
        container = by_service[service]
        if container.get("State") != "running":
            failures.append(f"{service} is {container.get('Status', container.get('State'))}")
        if service in {"broker", "schema-registry", "postgres"} and container.get("Health") != "healthy":
            failures.append(f"{service} health is {container.get('Health') or 'unknown'}")

    for service in SETUP_SERVICES:
        container = by_service[service]
        if container.get("State") != "exited" or int(container.get("ExitCode", -1)) != 0:
            failures.append(f"{service} did not complete successfully")
    if failures:
        raise ValueError("Compose services are not ready: " + "; ".join(failures))
    return containers


def fetch_event_tables():
    query = (
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
        "AND table_name IN ('mill_tool_event_reconciliation', 'mill_tool_event_state') ORDER BY table_name"
    )
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres", "psql", "--username", "gotogether",
            "--dbname", "gotogether", "--tuples-only", "--no-align", "--command", query,
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if result.returncode:
        raise RuntimeError(f"Could not inspect event PostgreSQL tables: {result.stderr.strip()}")
    return {name.strip() for name in result.stdout.splitlines() if name.strip()}


def fetch_grafana_dashboards():
    dashboards = set()
    for uid in ("base-streaming-platform", OUTCOME_DASHBOARD_UID):
        response = request_json(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
        if response.get("dashboard", {}).get("uid") != uid:
            raise RuntimeError(f"Grafana did not load the provisioned dashboard {uid}")
        dashboards.add(uid)
    return dashboards


def compose_logs():
    result = subprocess.run(
        ["docker", "compose", "logs", "--no-color", "--tail=200"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if result.returncode:
        raise RuntimeError(f"Could not inspect Compose logs: {result.stderr.strip()}")
    return result.stdout + result.stderr


def validate_service_logs(logs):
    failure = re.compile(r"Exception in thread|\bSEVERE:|\bERROR:|\bFATAL:", re.IGNORECASE)
    failures = [line for line in logs.splitlines() if failure.search(line)]
    if failures:
        raise ValueError("Service logs contain startup errors: " + " | ".join(failures[:5]))
    return logs


def check_tcp_endpoint(host, port):
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except OSError as error:
        raise RuntimeError(f"Endpoint {host}:{port} is unreachable: {error}") from error


def fetch_topic_names(bootstrap_server):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_server,
            client_id="gotogether-smoke-topic-check",
            request_timeout_ms=5000,
            bootstrap_timeout_ms=5000,
            enable_auto_commit=False,
        )
        try:
            return consumer.topics()
        finally:
            consumer.close()
    except KafkaError as error:
        raise RuntimeError(f"Kafka topic metadata request failed for {bootstrap_server}: {error}") from error


def validate_topic_names(topic_names):
    missing = set(REQUIRED_TOPICS) - set(topic_names)
    if missing:
        raise ValueError(f"Kafka topics are missing: {', '.join(sorted(missing))}")
    return topic_names


def validate_cut_subjects(subjects):
    missing = set(CUT_SUBJECTS) - set(subjects)
    if missing:
        raise ValueError(f"Cut Avro subjects are missing: {', '.join(sorted(missing))}")
    return subjects


def validate_cut_schemas(subjects, versions, canonical_schema):
    validate_cut_subjects(subjects)
    for subject, version in versions.items():
        if version.get("subject") != subject or version.get("schemaType") != "AVRO":
            raise ValueError(f"{subject} is not registered as the expected Avro value schema")
        try:
            registered_schema = json.loads(version["schema"])
        except (KeyError, json.JSONDecodeError) as error:
            raise ValueError(f"{subject} has invalid Avro schema text") from error
        if registered_schema != canonical_schema:
            raise ValueError(f"{subject} does not match the canonical cut Avro schema")
    return versions


def validate_event_schemas(subjects, versions, canonical_schema):
    missing = set(EVENT_SUBJECTS) - set(subjects)
    if missing:
        raise ValueError(f"Event Avro subjects are missing: {', '.join(sorted(missing))}")
    for subject in EVENT_SUBJECTS:
        version = versions.get(subject, {})
        if version.get("subject") != subject or version.get("schemaType") != "AVRO":
            raise ValueError(f"{subject} is not registered as the expected Avro event schema")
        try:
            registered_schema = json.loads(version["schema"])
        except (KeyError, json.JSONDecodeError) as error:
            raise ValueError(f"{subject} has invalid Avro schema text") from error
        if registered_schema != canonical_schema:
            raise ValueError(f"{subject} does not match the canonical event Avro schema")
    return versions


def validate_event_tables(table_names):
    required = {"mill_tool_event_reconciliation", "mill_tool_event_state"}
    missing = required - set(table_names)
    if missing:
        raise ValueError(f"Event tables are missing: {', '.join(sorted(missing))}")
    return table_names


def validate_grafana_dashboards(dashboard_uids):
    required = {"base-streaming-platform", OUTCOME_DASHBOARD_UID}
    missing = required - set(dashboard_uids)
    if missing:
        raise ValueError(f"Grafana dashboards are missing: {', '.join(sorted(missing))}")
    return set(dashboard_uids)


def validate_required_services(services):
    required = {
        "broker", "schema-registry", "topic-bootstrap", "cut-topic-bootstrap", "schema-bootstrap",
        "cuts-avro-schema-bootstrap", "cuts-db-bootstrap", "event-topic-bootstrap", "event-schema-bootstrap",
        "event-db-bootstrap", "cuts-app-build", "mill-cuts-lag-exporter", "mill-tool-events-outcomes",
        "postgres", "akhq", "prometheus", "grafana",
        "mill-cuts-transactional-producer", "mill-cuts-at-least-once-producer",
        "mill-cuts-transactional-consumer", "mill-cuts-idempotent-consumer",
        "mill-tool-events-producer", "mill-tool-events-consumer",
    }
    missing = required - set(services)
    if missing:
        raise ValueError(f"Compose services are missing from the default stack: {', '.join(sorted(missing))}")
    cut_producers = [service for service in services if service.startswith("mill-cuts-") and service.endswith("-producer")]
    cut_consumers = [service for service in services if service.startswith("mill-cuts-") and service.endswith("-consumer")]
    if len(cut_producers) != 2 or len(cut_consumers) != 2:
        raise ValueError(f"The default stack must contain exactly two cut producers and two cut consumers; found {len(cut_producers)} cut producers and {len(cut_consumers)} cut consumers")
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


def wait_for_prometheus(attempts=10, interval_seconds=2):
    last_error = None
    for _ in range(attempts):
        try:
            targets = request_json(f"{PROMETHEUS_URL}/api/v1/targets?state=active")
            if targets.get("status") != "success":
                raise RuntimeError(f"Prometheus target request failed: {targets}")
            target_status = validate_prometheus_targets(targets.get("data", {}).get("activeTargets", []))
            down_targets = [job for job, healthy in target_status.items() if not healthy]
            if down_targets:
                raise ValueError(f"Prometheus scrape targets are down: {', '.join(down_targets)}")
            validate_prometheus_metrics({metric: prometheus_query(metric) for metric in REQUIRED_METRICS})
            return target_status
        except (RuntimeError, ValueError) as error:
            last_error = error
            time.sleep(interval_seconds)
    raise RuntimeError(f"Prometheus targets or metrics did not become ready: {last_error}")


def main():
    try:
        canonical_schema = json.loads((ROOT / "schemas" / "cnc-demo-records.schema.json").read_text())
        canonical_avro = json.loads((ROOT / "src" / "main" / "resources" / "avro" / "cut-record.avsc").read_text())
        event_avro = json.loads((ROOT / "src" / "main" / "resources" / "avro" / "event-record.avsc").read_text())
        validate_required_services(compose_services())
        validate_service_states(compose_statuses())
        validate_service_logs(compose_logs())
        check_tcp_endpoint("127.0.0.1", int(os.environ.get("POSTGRES_PORT", "5432")))
        validate_event_tables(fetch_event_tables())
        offsets = validate_topic_offsets(topic_offsets())
        validate_cluster_metadata(fetch_cluster_metadata("127.0.0.1:9092"), "127.0.0.1")
        if not internal_cluster_metadata().strip():
            raise RuntimeError("Internal Kafka metadata request returned no broker data")
        subject_names = request_json(f"{REGISTRY_URL}/subjects")
        validate_registered_schema(
            subject_names,
            request_json(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/latest"),
            canonical_schema,
        )
        validate_cut_schemas(
            subject_names,
            {
                subject: request_json(f"{REGISTRY_URL}/subjects/{subject}/versions/latest")
                for subject in CUT_SUBJECTS
            },
            canonical_avro,
        )
        validate_event_schemas(
            subject_names,
            {
                subject: request_json(f"{REGISTRY_URL}/subjects/{subject}/versions/latest")
                for subject in EVENT_SUBJECTS
            },
            event_avro,
        )
        validate_topic_names(fetch_topic_names("127.0.0.1:9092"))
        validate_akhq_topic_names(request_json(f"{AKHQ_URL}/api/local/topic/name"))
        check_ready(f"{PROMETHEUS_URL}/-/ready")
        wait_for_prometheus()
        grafana_health = request_json(f"{GRAFANA_URL}/api/health")
        if grafana_health.get("database") != "ok":
            raise RuntimeError(f"Grafana is not healthy: {grafana_health}")
        validate_grafana_dashboards(fetch_grafana_dashboards())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Smoke check failed: {error}", file=sys.stderr)
        return 1

    print(f"Smoke check passed: all cut and event services are running, event topics, Avro subjects, and tables are ready, outcome metrics are reachable, both Grafana dashboards are provisioned, and the {len(offsets)}-partition canonical sample topic is empty.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
