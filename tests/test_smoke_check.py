import json
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import smoke_check
from scripts.smoke_check import (
    PROMETHEUS_JOBS,
    REQUIRED_METRICS,
    REQUIRED_TOPICS,
    RUNNING_SERVICES,
    SETUP_SERVICES,
    CUT_SUBJECTS,
    EVENT_SUBJECTS,
    EVENT_TOPICS,
    OUTCOME_DASHBOARD_UID,
    validate_akhq_topic_names,
    validate_cluster_metadata,
    validate_cut_schemas,
    validate_event_schemas,
    validate_event_tables,
    validate_grafana_dashboards,
    validate_prometheus_metrics,
    validate_prometheus_targets,
    validate_registered_schema,
    validate_required_services,
    validate_service_logs,
    validate_service_states,
    validate_topic_names,
    validate_topic_offsets,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schemas" / "cnc-demo-records.schema.json").read_text())
AVRO_SCHEMA = json.loads((ROOT / "src" / "main" / "resources" / "avro" / "cut-record.avsc").read_text())
EVENT_AVRO_SCHEMA = json.loads((ROOT / "src" / "main" / "resources" / "avro" / "event-record.avsc").read_text())
SUBJECT = "cnc-demo-records-value"


class SmokeCheckTests(unittest.TestCase):
    def test_topic_must_exist_and_have_only_zero_offsets(self):
        self.assertEqual(validate_topic_offsets("cnc-demo-records:0:0\n"), {0: 0})
        with self.assertRaisesRegex(ValueError, "no partition offsets"):
            validate_topic_offsets("")
        with self.assertRaisesRegex(ValueError, "nonzero offset"):
            validate_topic_offsets("cnc-demo-records:0:1\n")
        with self.assertRaisesRegex(ValueError, "one partition"):
            validate_topic_offsets("cnc-demo-records:0:0\ncnc-demo-records:1:0\n")

    def test_registered_schema_must_exist_and_match_canonical_json(self):
        version = {"subject": SUBJECT, "schemaType": "JSON", "schema": json.dumps(SCHEMA)}
        self.assertEqual(validate_registered_schema([SUBJECT], version, SCHEMA), SCHEMA)

        with self.assertRaisesRegex(ValueError, "subject is missing"):
            validate_registered_schema([], version, SCHEMA)

        wrong_schema = {**version, "schema": json.dumps({"type": "object"})}
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_registered_schema([SUBJECT], wrong_schema, SCHEMA)

    def test_cut_subjects_must_be_registered_with_canonical_avro_schema(self):
        versions = {
            subject: {"subject": subject, "schemaType": "AVRO", "schema": json.dumps(AVRO_SCHEMA)}
            for subject in CUT_SUBJECTS
        }
        self.assertEqual(validate_cut_schemas([*CUT_SUBJECTS, SUBJECT], versions, AVRO_SCHEMA), versions)
        with self.assertRaisesRegex(ValueError, "subjects are missing"):
            validate_cut_schemas([SUBJECT], versions, AVRO_SCHEMA)
        mismatched = {**versions, CUT_SUBJECTS[0]: {**versions[CUT_SUBJECTS[0]], "schemaType": "JSON"}}
        with self.assertRaisesRegex(ValueError, "not registered as the expected Avro"):
            validate_cut_schemas([*CUT_SUBJECTS, SUBJECT], mismatched, AVRO_SCHEMA)

    def test_event_topics_and_subjects_must_match_the_canonical_avro_schema(self):
        self.assertEqual(EVENT_TOPICS, ("mill-tool-events-source", "mill-tool-events-dlq"))
        self.assertEqual(EVENT_SUBJECTS, tuple(f"{topic}-value" for topic in EVENT_TOPICS))
        versions = {
            subject: {"subject": subject, "schemaType": "AVRO", "schema": json.dumps(EVENT_AVRO_SCHEMA)}
            for subject in EVENT_SUBJECTS
        }
        self.assertEqual(validate_event_schemas(EVENT_SUBJECTS, versions, EVENT_AVRO_SCHEMA), versions)
        with self.assertRaisesRegex(ValueError, "subjects are missing"):
            validate_event_schemas([], versions, EVENT_AVRO_SCHEMA)
        mismatched = {**versions, EVENT_SUBJECTS[0]: {**versions[EVENT_SUBJECTS[0]], "schemaType": "JSON"}}
        with self.assertRaisesRegex(ValueError, "not registered as the expected Avro"):
            validate_event_schemas(EVENT_SUBJECTS, mismatched, EVENT_AVRO_SCHEMA)

    def test_event_tables_and_both_provisioned_dashboards_are_required(self):
        self.assertTrue(validate_event_tables((
            "mill_tool_event_reconciliation",
            "mill_tool_event_state",
        )))
        with self.assertRaisesRegex(ValueError, "Event tables are missing"):
            validate_event_tables(("mill_tool_event_state",))
        self.assertEqual(
            validate_grafana_dashboards(("base-streaming-platform", OUTCOME_DASHBOARD_UID)),
            {"base-streaming-platform", OUTCOME_DASHBOARD_UID},
        )
        with self.assertRaisesRegex(ValueError, "dashboards are missing"):
            validate_grafana_dashboards(("base-streaming-platform",))

    def test_akhq_must_list_all_default_topics(self):
        self.assertEqual(set(validate_akhq_topic_names(REQUIRED_TOPICS)), set(REQUIRED_TOPICS))
        with self.assertRaisesRegex(ValueError, "Kafka topics are missing"):
            validate_akhq_topic_names(["cnc-demo-records"])

    def test_host_cluster_metadata_must_advertise_expected_broker(self):
        metadata = {"cluster_id": "demo-cluster", "brokers": [{"host": "127.0.0.1", "port": 9092}]}
        self.assertEqual(validate_cluster_metadata(metadata, "127.0.0.1"), metadata)
        with self.assertRaisesRegex(ValueError, "did not advertise broker host"):
            validate_cluster_metadata(metadata, "broker")

    def test_host_metadata_client_uses_supported_bootstrap_timeout(self):
        metadata = {"cluster_id": "demo-cluster", "brokers": [{"host": "127.0.0.1", "port": 9092}]}
        with patch("scripts.smoke_check.KafkaAdminClient") as client:
            client.return_value.describe_cluster.return_value = metadata
            self.assertEqual(smoke_check.fetch_cluster_metadata("127.0.0.1:9092"), metadata)
        self.assertEqual(client.call_args.kwargs["bootstrap_timeout_ms"], 5000)
        self.assertNotIn("api_version_auto_timeout_ms", client.call_args.kwargs)
        client.return_value.close.assert_called_once_with()

    def test_prometheus_target_health_distinguishes_down_from_up(self):
        targets = [
            {"labels": {"job": job}, "health": "down" if job == "schema-registry" else "up"}
            for job in PROMETHEUS_JOBS
        ]
        health = validate_prometheus_targets(targets)
        self.assertFalse(health["schema-registry"])
        self.assertTrue(all(health[job] for job in PROMETHEUS_JOBS if job != "schema-registry"))
        with self.assertRaisesRegex(ValueError, "target is missing"):
            validate_prometheus_targets(targets[:-1])

    def test_prometheus_requires_each_live_metric_series(self):
        results = {name: [{"metric": {"__name__": name}, "value": [0, "0"]}] for name in REQUIRED_METRICS}
        self.assertEqual(validate_prometheus_metrics(results), results)
        with self.assertRaisesRegex(ValueError, "has no live series"):
            validate_prometheus_metrics({})

    def test_startup_smoke_requires_running_services_and_successful_setup_jobs(self):
        containers = [
            {"Service": name, "State": "running", "Health": "healthy" if name in {"broker", "schema-registry", "postgres"} else ""}
            for name in RUNNING_SERVICES
        ] + [{"Service": name, "State": "exited", "ExitCode": 0} for name in SETUP_SERVICES]
        self.assertTrue(validate_service_states(containers))

        failed = [*containers, {"Service": "mill-cuts-idempotent-consumer", "State": "exited", "ExitCode": 1}]
        with self.assertRaisesRegex(ValueError, "is exited"):
            validate_service_states(failed)

    def test_startup_smoke_checks_logs_and_expected_topics(self):
        self.assertTrue(validate_service_logs("broker started\\nproducer ready\\n"))
        with self.assertRaisesRegex(ValueError, "startup errors"):
            validate_service_logs("Exception in thread \\\"main\\\" failed")
        self.assertEqual(validate_topic_names(REQUIRED_TOPICS), REQUIRED_TOPICS)
        with self.assertRaisesRegex(ValueError, "topics are missing"):
            validate_topic_names(["cnc-demo-records"])

    def test_full_stack_requires_application_and_database_services(self):
        services = [
            "broker", "schema-registry", "topic-bootstrap", "cut-topic-bootstrap",
            "schema-bootstrap", "cuts-avro-schema-bootstrap", "cuts-db-bootstrap", "event-topic-bootstrap",
            "event-schema-bootstrap", "event-db-bootstrap", "postgres", "akhq", "prometheus", "grafana",
            "mill-cuts-transactional-producer", "mill-cuts-at-least-once-producer",
            "mill-cuts-transactional-consumer", "mill-cuts-idempotent-consumer", "cuts-app-build",
            "mill-cuts-lag-exporter", "mill-tool-events-producer", "mill-tool-events-consumer", "mill-tool-events-outcomes",
        ]
        self.assertEqual(validate_required_services(services), services)
        with self.assertRaisesRegex(ValueError, "missing from the default stack"):
            validate_required_services(["broker", "postgres"])
        with self.assertRaisesRegex(ValueError, "exactly two cut producers and two cut consumers"):
            validate_required_services(services + ["mill-cuts-extra-consumer"])


if __name__ == "__main__":
    unittest.main()
