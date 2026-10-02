import json
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import smoke_check
from scripts.smoke_check import (
    REQUIRED_METRICS,
    validate_akhq_topic_names,
    validate_cluster_metadata,
    validate_prometheus_metrics,
    validate_prometheus_targets,
    validate_registered_schema,
    validate_service_scope,
    validate_topic_offsets,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schemas" / "cnc-demo-records.schema.json").read_text())
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

    def test_akhq_must_list_the_sample_topic(self):
        self.assertIn("cnc-demo-records", validate_akhq_topic_names(["cnc-demo-records"]))
        with self.assertRaisesRegex(ValueError, "does not list topic"):
            validate_akhq_topic_names([])

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
            {"labels": {"job": "kafka-broker"}, "health": "up"},
            {"labels": {"job": "schema-registry"}, "health": "down"},
        ]
        self.assertEqual(validate_prometheus_targets(targets), {"kafka-broker": True, "schema-registry": False})
        with self.assertRaisesRegex(ValueError, "target is missing"):
            validate_prometheus_targets(targets[:1])

    def test_prometheus_requires_each_live_metric_series(self):
        results = {name: [{"metric": {"__name__": name}, "value": [0, "0"]}] for name in REQUIRED_METRICS}
        self.assertEqual(validate_prometheus_metrics(results), results)
        with self.assertRaisesRegex(ValueError, "has no live series"):
            validate_prometheus_metrics({})

    def test_base_stack_excludes_application_and_database_services(self):
        self.assertEqual(validate_service_scope(["broker", "topic-bootstrap", "akhq"]), ["broker", "topic-bootstrap", "akhq"])
        with self.assertRaisesRegex(ValueError, "Unexpected application services"):
            validate_service_scope(["broker", "producer-demo"])


if __name__ == "__main__":
    unittest.main()
