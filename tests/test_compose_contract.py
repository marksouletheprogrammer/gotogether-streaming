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

    def test_single_node_kraft_and_default_cut_application_services(self):
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
        required = {
            "mill-cuts-transactional-producer",
            "mill-cuts-at-least-once-producer",
            "mill-cuts-transactional-consumer",
            "mill-cuts-idempotent-consumer",
            "cuts-app-build",
            "mill-cuts-lag-exporter",
            "postgres",
        }
        self.assertTrue(required <= set(self.services), f"Default stack is missing services: {required - set(self.services)}")
        self.assertEqual(sum(name.endswith("-producer") for name in self.services), 2)
        self.assertEqual(sum(name.endswith("-consumer") for name in self.services), 2)
        self.assertIsNone(self.services["mill-cuts-transactional-producer"].get("profiles"))
        self.assertIsNone(self.services["mill-cuts-at-least-once-producer"].get("profiles"))
        self.assertIsNone(self.services["mill-cuts-transactional-consumer"].get("profiles"))
        self.assertIsNone(self.services["mill-cuts-idempotent-consumer"].get("profiles"))

    def test_cut_producers_have_distinct_topics_clients_and_matched_rates(self):
        transactional = self.services["mill-cuts-transactional-producer"]
        at_least_once = self.services["mill-cuts-at-least-once-producer"]
        transactional_env = transactional["environment"]
        at_least_once_env = at_least_once["environment"]

        self.assertEqual(transactional_env["CUT_TOPIC"], "mill-cuts-transactional-source")
        self.assertEqual(at_least_once_env["CUT_TOPIC"], "mill-cuts-replay-source")
        self.assertNotEqual(transactional_env["KAFKA_CLIENT_ID"], at_least_once_env["KAFKA_CLIENT_ID"])
        self.assertEqual(transactional_env["CUT_RECORD_COUNT"], at_least_once_env["CUT_RECORD_COUNT"])
        self.assertEqual(transactional_env["CUT_INTERVAL_MS"], at_least_once_env["CUT_INTERVAL_MS"])
        self.assertEqual(transactional["command"], ["producer", "transactional"])
        self.assertEqual(at_least_once["command"], ["producer", "at-least-once"])

    def test_group_lag_exporter_is_default_started_after_kafka_and_java_build(self):
        exporter = self.services["mill-cuts-lag-exporter"]
        self.assertEqual(exporter["environment"]["LAG_EXPORTER_PORT"], "9407")
        self.assertEqual(exporter["depends_on"]["broker"]["condition"], "service_healthy")
        self.assertEqual(exporter["depends_on"]["cuts-app-build"]["condition"], "service_completed_successfully")
        self.assertIsNone(exporter.get("profiles"))

    def test_postgres_is_default_networked_persistent_and_loopback_published(self):
        database = self.services["postgres"]
        self.assertTrue(any(port.get("host_ip") == "127.0.0.1" and port.get("target") == 5432 for port in database["ports"]))
        self.assertEqual(database["networks"], ["streaming"])
        self.assertEqual(database["restart"], "unless-stopped")
        self.assertTrue(any("/var/lib/postgresql/data" in volume for volume in database["volumes"]))
        self.assertIn("pg_isready", " ".join(database["healthcheck"]["test"]))
        self.assertEqual(database["environment"]["POSTGRES_HOST_AUTH_METHOD"], "trust")

    def test_cut_applications_wait_for_kafka_registry_and_postgres(self):
        for name in (
            "mill-cuts-transactional-producer",
            "mill-cuts-at-least-once-producer",
            "mill-cuts-transactional-consumer",
            "mill-cuts-idempotent-consumer",
        ):
            with self.subTest(service=name):
                dependencies = self.services[name]["depends_on"]
                self.assertEqual(dependencies["broker"]["condition"], "service_healthy")
                self.assertEqual(dependencies["schema-registry"]["condition"], "service_healthy")
                self.assertEqual(dependencies["postgres"]["condition"], "service_healthy")

    def test_cut_topics_and_tables_are_bootstrapped_idempotently(self):
        cut_topics = self.services["cut-topic-bootstrap"]
        self.assertEqual(cut_topics["image"], "confluentinc/cp-kafka:8.1.6")
        self.assertEqual(cut_topics["depends_on"]["broker"]["condition"], "service_healthy")
        self.assertIn("--if-not-exists", cut_topics["command"][0])
        for topic in (
            "mill-cuts-transactional-source",
            "mill-cuts-replay-source",
            "mill-cuts-committed",
        ):
            self.assertIn(topic, cut_topics["command"][0])

        database_setup = self.services["cuts-db-bootstrap"]
        self.assertEqual(database_setup["depends_on"]["postgres"]["condition"], "service_healthy")
        self.assertIn("./scripts/init_cuts.sql:/sql/init_cuts.sql:ro", database_setup["volumes"])
        schema_sql = (ROOT / "scripts" / "init_cuts.sql").read_text()
        self.assertIn("CREATE TABLE IF NOT EXISTS mill_cuts_transactional_writes", schema_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS mill_cuts_idempotent_writes", schema_sql)
        self.assertIn("event_id TEXT PRIMARY KEY", schema_sql)
        repository = (ROOT / "src" / "main" / "java" / "com" / "improving" / "gotogether" / "cuts" / "PostgresCutRepository.java").read_text()
        self.assertIn("ON CONFLICT (event_id) DO UPDATE", repository)
        self.assertIn("payload = EXCLUDED.payload", repository)

        avro_setup = self.services["cuts-avro-schema-bootstrap"]
        self.assertEqual(avro_setup["image"], "python:3.13.7-slim")
        self.assertEqual(avro_setup["depends_on"]["schema-registry"]["condition"], "service_healthy")
        self.assertEqual(avro_setup["depends_on"]["cut-topic-bootstrap"]["condition"], "service_completed_successfully")
        self.assertIn("./scripts/register_cut_schemas.py:/scripts/register_cut_schemas.py:ro", avro_setup["volumes"])
        self.assertIn("./src/main/resources/avro/cut-record.avsc:/schema/cut-record.avsc:ro", avro_setup["volumes"])

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
