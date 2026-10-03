import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
MONITORING = ROOT / "monitoring"
PROMETHEUS_CONFIG = MONITORING / "prometheus.yml"
DATASOURCE_CONFIG = MONITORING / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
DASHBOARD_PROVIDER = MONITORING / "grafana" / "provisioning" / "dashboards" / "provider.yml"
DASHBOARD = MONITORING / "grafana" / "dashboards" / "base-streaming-platform.json"


class MonitoringContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for path in (PROMETHEUS_CONFIG, DATASOURCE_CONFIG, DASHBOARD_PROVIDER, DASHBOARD):
            if not path.is_file():
                raise AssertionError(f"Missing monitoring configuration: {path}")
        cls.prometheus = yaml.safe_load(PROMETHEUS_CONFIG.read_text())
        cls.datasource = yaml.safe_load(DATASOURCE_CONFIG.read_text())
        cls.provider = yaml.safe_load(DASHBOARD_PROVIDER.read_text())
        cls.dashboard = json.loads(DASHBOARD.read_text())
        cls.compose = yaml.safe_load((ROOT / "compose.yaml").read_text())

    def test_broker_and_registry_are_separate_prometheus_targets(self):
        jobs = {job["job_name"]: job for job in self.prometheus["scrape_configs"]}
        self.assertIn("kafka-broker", jobs)
        self.assertIn("schema-registry", jobs)
        self.assertIn("broker:9404", jobs["kafka-broker"]["static_configs"][0]["targets"])
        self.assertIn("schema-registry:9405", jobs["schema-registry"]["static_configs"][0]["targets"])

    def test_dashboard_datasource_and_required_live_series_are_provisioned(self):
        datasource = self.datasource["datasources"][0]
        self.assertEqual(datasource["uid"], "prometheus")
        self.assertEqual(datasource["type"], "prometheus")
        self.assertTrue(self.provider["providers"])
        expressions = {
            target["expr"]
            for panel in self.dashboard["panels"]
            for target in panel.get("targets", [])
        }
        required = {
            'up{job="kafka-broker"}',
            'up{job="schema-registry"}',
            "rate(kafka_server_broker_topic_metrics_bytes_in_total[5m])",
            "kafka_network_request_metrics_request_total_time_ms_mean",
            "kafka_schema_registry_jersey_request_rate",
            "kafka_schema_registry_jersey_request_error_rate",
        }
        self.assertTrue(required <= expressions, f"Dashboard is missing expressions: {required - expressions}")

    def test_cut_metrics_have_default_scrape_targets_and_existing_dashboard_panels(self):
        jobs = {job["job_name"]: job for job in self.prometheus["scrape_configs"]}
        targets = {
            job: jobs[job]["static_configs"][0]["targets"]
            for job in (
                "mill-cuts-transactional-producer",
                "mill-cuts-at-least-once-producer",
                "mill-cuts-transactional-consumer",
                "mill-cuts-idempotent-consumer",
                "mill-cuts-group-lag",
            )
        }
        self.assertEqual(targets["mill-cuts-transactional-producer"], ["mill-cuts-transactional-producer:9406"])
        self.assertEqual(targets["mill-cuts-at-least-once-producer"], ["mill-cuts-at-least-once-producer:9406"])
        self.assertEqual(targets["mill-cuts-transactional-consumer"], ["mill-cuts-transactional-consumer:9406"])
        self.assertEqual(targets["mill-cuts-idempotent-consumer"], ["mill-cuts-idempotent-consumer:9406"])
        self.assertEqual(targets["mill-cuts-group-lag"], ["mill-cuts-lag-exporter:9407"])

        expressions = {
            target["expr"]
            for panel in self.dashboard["panels"]
            for target in panel.get("targets", [])
        }
        required = {
            'rate(mill_cuts_producer_records_sent_total{client_id="mill-cuts-transactional-producer"}[1m])',
            'rate(mill_cuts_producer_send_errors_total{client_id="mill-cuts-transactional-producer"}[1m])',
            'rate(mill_cuts_producer_records_sent_total{client_id="mill-cuts-at-least-once-producer"}[1m])',
            'rate(mill_cuts_producer_send_errors_total{client_id="mill-cuts-at-least-once-producer"}[1m])',
            'mill_cuts_consumer_group_lag{group_id="mill-cuts-transactional-consumer"}',
            'rate(mill_cuts_consumer_records_processed_total{group_id="mill-cuts-transactional-consumer"}[1m])',
            'rate(mill_cuts_consumer_processing_errors_total{group_id="mill-cuts-transactional-consumer"}[1m])',
            'mill_cuts_consumer_group_lag{group_id="mill-cuts-idempotent-consumer"}',
            'rate(mill_cuts_consumer_records_processed_total{group_id="mill-cuts-idempotent-consumer"}[1m])',
            'rate(mill_cuts_consumer_processing_errors_total{group_id="mill-cuts-idempotent-consumer"}[1m])',
        }
        self.assertTrue(required <= expressions, f"Existing dashboard is missing cut panels: {required - expressions}")
        self.assertEqual(self.dashboard["uid"], "base-streaming-platform")

    def test_jmx_agents_are_pinned_checksum_verified_and_bounded(self):
        downloader = (ROOT / "scripts" / "download_jmx_exporter.py").read_text()
        dockerfile = (ROOT / "Dockerfile.jmx").read_text()
        self.assertIn('VERSION = "1.6.0"', downloader)
        self.assertIn("a95983fd96e865d2bcdf911cc500e7c82808c27ab9fd226bf96732b6c3d8c46e", downloader)
        self.assertIn("hashlib.sha256(artifact).hexdigest()", downloader)
        self.assertIn("COPY --from=jmx-agent", dockerfile)
        app_dockerfile = (ROOT / "Dockerfile.app").read_text()
        self.assertIn("COPY --from=jmx-agent", app_dockerfile)

        for service_name in (
            "mill-cuts-transactional-producer",
            "mill-cuts-at-least-once-producer",
            "mill-cuts-transactional-consumer",
            "mill-cuts-idempotent-consumer",
        ):
            with self.subTest(service=service_name):
                service = self.compose["services"][service_name]
                self.assertIn("=9406:/etc/jmx/cut-app.yml", " ".join(service["entrypoint"]))
                self.assertIn("./monitoring/cut-app-jmx.yml:/etc/jmx/cut-app.yml:ro", service["volumes"])

        broker = self.compose["services"]["broker"]
        registry = self.compose["services"]["schema-registry"]
        self.assertEqual(broker["build"]["args"]["CONFLUENT_IMAGE"], "confluentinc/cp-kafka:8.1.6")
        self.assertEqual(registry["build"]["args"]["CONFLUENT_IMAGE"], "confluentinc/cp-schema-registry:8.1.6")
        self.assertIn("=9404:/etc/jmx/kafka.yml", broker["environment"]["KAFKA_OPTS"])
        self.assertIn("=9405:/etc/jmx/schema-registry.yml", registry["environment"]["SCHEMA_REGISTRY_JMX_OPTS"])
        for filename in ("kafka-jmx.yml", "schema-registry-jmx.yml", "cut-app-jmx.yml"):
            config = yaml.safe_load((MONITORING / filename).read_text())
            self.assertLessEqual(len(config["includeObjectNames"]), 4)
            self.assertTrue(config["rules"])

        cut_metrics = yaml.safe_load((MONITORING / "cut-app-jmx.yml").read_text())
        self.assertEqual({rule["name"] for rule in cut_metrics["rules"]}, {
            "mill_cuts_producer_records_sent_total",
            "mill_cuts_producer_send_errors_total",
            "mill_cuts_consumer_records_processed_total",
            "mill_cuts_consumer_processing_errors_total",
        })

    def test_metrics_services_are_loopback_published_with_disposable_storage(self):
        services = self.compose["services"]
        prometheus = services["prometheus"]
        grafana = services["grafana"]
        self.assertEqual(prometheus["image"], "prom/prometheus:v3.14.0")
        self.assertEqual(grafana["image"], "grafana/grafana:13.0.9")
        self.assertTrue(any(str(port).startswith("127.0.0.1:9090:") for port in prometheus["ports"]))
        self.assertIn("prometheus_data", self.compose["volumes"])
        self.assertIn("grafana_data", self.compose["volumes"])
        self.assertTrue(any(str(port).startswith("127.0.0.1:3000:") for port in grafana["ports"]))
        self.assertEqual(grafana["environment"]["GF_AUTH_ANONYMOUS_ORG_ROLE"], "Viewer")
        self.assertEqual(grafana["environment"]["GF_AUTH_ANONYMOUS_ENABLED"], "true")

    def test_dashboard_exposes_missing_telemetry_and_uses_every_exported_series(self):
        expressions = {
            target["expr"]
            for panel in self.dashboard["panels"]
            for target in panel.get("targets", [])
        }
        for filename in ("kafka-jmx.yml", "schema-registry-jmx.yml", "cut-app-jmx.yml"):
            rules = yaml.safe_load((MONITORING / filename).read_text())["rules"]
            for rule in rules:
                with self.subTest(metric=rule["name"]):
                    self.assertTrue(any(rule["name"] in expression for expression in expressions))

        traffic = next(panel for panel in self.dashboard["panels"] if panel["title"] == "Kafka throughput")
        self.assertEqual(traffic["fieldConfig"]["defaults"]["noValue"], "No data")
        self.assertIn("zero", traffic["description"].lower())
        self.assertIn("missing", traffic["description"].lower())

    def test_runbook_commands_and_endpoints_match_checked_in_stack(self):
        readme = (ROOT / "README.md").read_text()
        for value in (
            "python3 -m venv .venv",
            "python -m pip install -r requirements-test.txt",
            "python -m unittest discover -s tests -v",
            "docker compose config",
            "docker compose up -d --build",
            "docker compose ps -a",
            "python scripts/smoke_check.py",
            "docker compose down --volumes",
            "127.0.0.1:9092",
            "http://localhost:8080",
            "http://localhost:8081",
            "http://localhost:9090",
            "http://localhost:3000",
        ):
            with self.subTest(value=value):
                self.assertIn(value, readme)
        self.assertIn("permanently deletes", readme)
        self.assertIn("kafka_data", self.compose["volumes"])
        self.assertIn("prometheus_data", self.compose["volumes"])
        self.assertIn("grafana_data", self.compose["volumes"])


if __name__ == "__main__":
    unittest.main()
