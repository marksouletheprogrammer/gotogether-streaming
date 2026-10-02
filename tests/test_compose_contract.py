import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = ROOT / "compose.yaml"


class ComposeContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not COMPOSE_PATH.is_file():
            raise AssertionError(f"Missing Compose stack: {COMPOSE_PATH}")
        cls.compose = yaml.safe_load(COMPOSE_PATH.read_text())
        cls.services = cls.compose["services"]

    def test_single_node_kraft_listeners_and_demo_scope(self):
        broker = self.services["broker"]
        environment = broker["environment"]
        self.assertIn("broker,controller", environment["KAFKA_PROCESS_ROLES"])
        self.assertIn("broker:29092", environment["KAFKA_ADVERTISED_LISTENERS"])
        self.assertIn("127.0.0.1:9092", environment["KAFKA_ADVERTISED_LISTENERS"])
        self.assertTrue(any(str(port).startswith("127.0.0.1:9092:") for port in broker["ports"]))
        self.assertTrue(any(source in self.compose["volumes"] for source, _ in (
            volume.split(":", 1) for volume in broker["volumes"]
        )))
        self.assertNotIn("zookeeper", self.services)
        self.assertFalse({"producer", "consumer", "database"} & set(self.services))

    def test_health_checks_use_commands_available_in_service_images(self):
        broker_check = " ".join(self.services["broker"]["healthcheck"]["test"])
        registry_check = " ".join(self.services["schema-registry"]["healthcheck"]["test"])
        self.assertIn("kafka-topics", broker_check)
        self.assertIn("KAFKA_OPTS=", broker_check)
        self.assertIn("/dev/tcp/127.0.0.1/8081", registry_check)
        self.assertNotIn("cub", broker_check + registry_check)

    def test_akhq_is_loopback_only_and_connected_to_broker_and_registry(self):
        akhq = self.services["akhq"]
        self.assertEqual(akhq["image"], "tchiotludo/akhq:0.28.0")
        self.assertTrue(any(str(port).startswith("127.0.0.1:8080:") for port in akhq["ports"]))
        configuration = akhq["environment"]["AKHQ_CONFIGURATION"]
        self.assertIn("bootstrap.servers: broker:29092", configuration)
        self.assertIn("url: http://schema-registry:8081", configuration)
        self.assertEqual(akhq["depends_on"]["broker"]["condition"], "service_healthy")
        self.assertEqual(akhq["depends_on"]["schema-registry"]["condition"], "service_healthy")

    def test_long_running_services_share_restart_and_network_settings(self):
        for name in ("broker", "schema-registry", "akhq"):
            with self.subTest(service=name):
                self.assertEqual(self.services[name]["restart"], "unless-stopped")
                self.assertEqual(self.services[name]["networks"], ["streaming"])

    def test_topic_and_schema_bootstrap_jobs_are_health_gated(self):
        topic_setup = self.services["topic-bootstrap"]
        self.assertEqual(topic_setup["image"], "confluentinc/cp-kafka:8.1.6")
        self.assertEqual(topic_setup["depends_on"]["broker"]["condition"], "service_healthy")
        self.assertEqual(topic_setup["entrypoint"], ["kafka-topics"])
        command = topic_setup["command"]
        self.assertIn("--if-not-exists", command)
        self.assertEqual(command[command.index("--topic") + 1], "cnc-demo-records")
        self.assertEqual(command[command.index("--partitions") + 1], "1")
        self.assertEqual(command[command.index("--replication-factor") + 1], "1")
        self.assertEqual(command[command.index("--config") + 1], "retention.ms=86400000")

        schema_setup = self.services["schema-bootstrap"]
        self.assertEqual(schema_setup["image"], "python:3.13.7-slim")
        self.assertEqual(schema_setup["depends_on"]["schema-registry"]["condition"], "service_healthy")
        self.assertEqual(schema_setup["depends_on"]["topic-bootstrap"]["condition"], "service_completed_successfully")
        self.assertIn("./schemas/cnc-demo-records.schema.json:/schema/cnc-demo-records.schema.json:ro", schema_setup["volumes"])


if __name__ == "__main__":
    unittest.main()
