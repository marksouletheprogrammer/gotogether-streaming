import json
import subprocess
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
        self.assertGreater(self.provider["providers"][0].get("updateIntervalSeconds", 10), 10)
        expressions = {
            target["expr"]
            for panel in self.dashboard["panels"]
            for target in panel.get("targets", [])
        }
        required = {
            'up{job="kafka-broker"}',
            'up{job="schema-registry"}',
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
            "mill_cuts_producer_records_sent_total",
            "mill_tool_events_producer_records_sent_total",
            "mill_cuts_producer_send_errors_total",
            "mill_tool_events_producer_send_errors_total",
            "mill_cuts_consumer_records_processed_total",
            "mill_tool_events_consumer_records_processed_total",
            "mill_cuts_consumer_processing_errors_total",
            "mill_tool_events_consumer_processing_errors_total",
            "mill_cuts_consumer_group_lag",
        }
        joined_expressions = " ".join(expressions)
        missing = {metric for metric in required if metric not in joined_expressions}
        self.assertFalse(missing, f"Existing dashboard is missing cut metrics: {missing}")
        self.assertEqual(self.dashboard["uid"], "base-streaming-platform")

    def test_dashboard_variables_filter_expected_panels_and_default_to_all(self):
        variables = {variable["name"]: variable for variable in self.dashboard["templating"]["list"]}
        expected_variables = {
            "producer_client_id": "client_id",
            "consumer_group_id": "group_id",
            "topic": "topic",
        }
        self.assertEqual(set(variables), set(expected_variables))
        for name, label in expected_variables.items():
            with self.subTest(variable=name):
                variable = variables[name]
                self.assertTrue(variable["includeAll"])
                self.assertEqual(variable["allValue"], ".*")
                self.assertEqual(variable["current"]["value"], "$__all")
                self.assertIn(label, variable["query"])

        panels = {panel["title"]: panel for panel in self.dashboard["panels"]}
        for title in ("Kafka throughput", "Producer throughput", "Producer send errors"):
            expression = " ".join(target["expr"] for target in panels[title]["targets"])
            self.assertIn('client_id=~"${producer_client_id:regex}"', expression)
            self.assertNotIn("$topic", expression)
        for title in ("Consumer throughput", "Consumer processing errors"):
            expression = " ".join(target["expr"] for target in panels[title]["targets"])
            self.assertIn('group_id=~"${consumer_group_id:regex}"', expression)
            self.assertNotIn("$topic", expression)
        lag_expression = " ".join(target["expr"] for target in panels["Consumer group lag"]["targets"])
        self.assertIn('group_id=~"${consumer_group_id:regex}"', lag_expression)
        self.assertIn('topic=~"${topic:regex}"', lag_expression)

    def test_pipeline_panels_separate_signals_with_matching_units_and_labels(self):
        panels = {panel["title"]: panel for panel in self.dashboard["panels"]}
        required_metrics = {
            "Producer throughput": (
                "mill_cuts_producer_records_sent_total",
                "mill_tool_events_producer_records_sent_total",
            ),
            "Producer send errors": (
                "mill_cuts_producer_send_errors_total",
                "mill_tool_events_producer_send_errors_total",
            ),
            "Consumer throughput": (
                "mill_cuts_consumer_records_processed_total",
                "mill_tool_events_consumer_records_processed_total",
            ),
            "Consumer processing errors": (
                "mill_cuts_consumer_processing_errors_total",
                "mill_tool_events_consumer_processing_errors_total",
            ),
            "Consumer group lag": ("mill_cuts_consumer_group_lag",),
        }
        self.assertTrue(set(required_metrics) <= set(panels), f"Missing separate signal panels: {set(required_metrics) - set(panels)}")
        for title, metrics in required_metrics.items():
            with self.subTest(panel=title):
                panel = panels[title]
                self.assertEqual(panel["type"], "timeseries")
                expression = " ".join(target["expr"] for target in panel["targets"])
                for metric in metrics:
                    self.assertIn(metric, expression)

        self.assertEqual(panels["Producer throughput"]["fieldConfig"]["defaults"]["unit"], "ops")
        self.assertEqual(panels["Producer send errors"]["fieldConfig"]["defaults"]["unit"], "ops")
        self.assertEqual(panels["Consumer throughput"]["fieldConfig"]["defaults"]["unit"], "ops")
        self.assertEqual(panels["Consumer processing errors"]["fieldConfig"]["defaults"]["unit"], "ops")
        self.assertEqual(panels["Consumer group lag"]["fieldConfig"]["defaults"]["unit"], "short")
        for title in (
            "Producer throughput",
            "Producer send errors",
            "Consumer throughput",
            "Consumer processing errors",
        ):
            with self.subTest(rate_panel=title):
                panel = panels[title]
                self.assertTrue(panel["description"])
                for target in panel["targets"]:
                    self.assertIn("[5m]", target["expr"])
                    self.assertTrue(target["legendFormat"])

        rules = {rule["name"]: rule for rule in yaml.safe_load((MONITORING / "cut-app-jmx.yml").read_text())["rules"]}
        self.assertEqual(rules["mill_cuts_producer_records_sent_total"]["labels"], {"client_id": "$1"})
        self.assertEqual(rules["mill_cuts_consumer_records_processed_total"]["labels"], {"group_id": "$1"})
        lag_exporter = (ROOT / "src/main/java/com/improving/gotogether/cuts/ConsumerLagExporter.java").read_text()
        self.assertIn('append("\\\",topic=\\\"")', lag_exporter)
        self.assertIn('append(escape(sample.groupId()))', lag_exporter)

    def test_component_health_panels_have_healthy_unhealthy_and_unavailable_states(self):
        expected_health_jobs = {
            "Kafka broker health": "kafka-broker",
            "Schema Registry health": "schema-registry",
            "Transactional cut producer health": "mill-cuts-transactional-producer",
            "At-least-once cut producer health": "mill-cuts-at-least-once-producer",
            "Transactional cut consumer health": "mill-cuts-transactional-consumer",
            "Idempotent cut consumer health": "mill-cuts-idempotent-consumer",
            "Consumer lag exporter health": "mill-cuts-group-lag",
            "Technician event producer health": "mill-tool-events-producer",
            "Technician event consumer health": "mill-tool-events-consumer",
            "Event outcomes exporter health": "mill-tool-events-outcomes",
        }
        panels = {panel["title"]: panel for panel in self.dashboard["panels"]}
        self.assertTrue(set(expected_health_jobs) <= set(panels), f"Missing component health panels: {set(expected_health_jobs) - set(panels)}")
        for title, job in expected_health_jobs.items():
            with self.subTest(component=title):
                panel = panels[title]
                self.assertEqual(panel["type"], "stat")
                self.assertEqual(panel["options"]["textMode"], "value")
                self.assertEqual(panel["targets"][0]["expr"], f'up{{job="{job}"}}')
                defaults = panel["fieldConfig"]["defaults"]
                self.assertEqual(defaults["noValue"], "Unavailable")
                value_mapping = next(mapping for mapping in defaults["mappings"] if mapping["type"] == "value")
                self.assertEqual(value_mapping["options"]["1"]["text"], "Healthy")
                self.assertEqual(value_mapping["options"]["1"]["color"], "green")
                self.assertEqual(value_mapping["options"]["0"]["text"], "Unhealthy")
                self.assertEqual(value_mapping["options"]["0"]["color"], "red")
                unavailable = next(mapping for mapping in defaults["mappings"] if mapping["type"] == "special")
                self.assertEqual(unavailable["options"]["match"], "null")
                self.assertEqual(unavailable["options"]["result"]["text"], "Unavailable")
                self.assertEqual(unavailable["options"]["result"]["color"], "gray")

    def test_broker_and_registry_panels_use_readable_legends_and_client_series(self):
        panels = {panel["title"]: panel for panel in self.dashboard["panels"]}
        self.assertNotIn("Kafka broker availability", panels)
        self.assertNotIn("Schema Registry availability", panels)
        traffic = panels["Kafka throughput"]
        self.assertEqual(traffic["fieldConfig"]["defaults"]["unit"], "ops")
        self.assertEqual(len(traffic["targets"]), 2)
        for target, metric in zip(traffic["targets"], (
            "mill_cuts_producer_records_sent_total", "mill_tool_events_producer_records_sent_total"
        )):
            self.assertIn(metric, target["expr"])
            self.assertIn('client_id=~"${producer_client_id:regex}"', target["expr"])
            self.assertEqual(target["legendFormat"], "{{client_id}}")
        kafka_rules = yaml.safe_load((MONITORING / "kafka-jmx.yml").read_text())["rules"]
        latency_rule = next(rule for rule in kafka_rules if rule["name"] == "kafka_network_request_metrics_request_total_time_ms_mean")
        self.assertIn("request", latency_rule["labels"])
        self.assertEqual(panels["Kafka request latency"]["targets"][0]["legendFormat"], "{{request}}")
        self.assertEqual(panels["Kafka under-replicated partitions"]["targets"][0]["legendFormat"], "under-replicated")
        self.assertEqual(panels["Schema Registry requests per second"]["targets"][0]["legendFormat"], "requests")
        self.assertEqual(panels["Schema Registry request errors per second"]["targets"][0]["legendFormat"], "errors")

    def test_dashboard_groups_panels_into_consistent_operational_sections(self):
        panels = self.dashboard["panels"]
        row_positions = [index for index, panel in enumerate(panels) if panel["type"] == "row"]
        rows = [panels[index] for index in row_positions]
        self.assertEqual(
            [row["title"] for row in rows],
            ["Broker and registry", "Producer activity", "Consumer activity and lag", "Component health"],
        )
        next_y = 0
        for section, position in enumerate(row_positions):
            row = rows[section]
            height = 4 if row["title"] == "Component health" else 6
            following = row_positions[section + 1] if section + 1 < len(rows) else len(panels)
            charts = panels[position + 1:following]
            self.assertTrue(charts)
            self.assertFalse(row["collapsed"])
            self.assertEqual(row["gridPos"], {"h": 1, "w": 24, "x": 0, "y": next_y})
            for index, panel in enumerate(charts):
                with self.subTest(panel=panel["title"]):
                    self.assertEqual(panel["gridPos"], {
                        "h": height, "w": 8, "x": (index % 3) * 8,
                        "y": next_y + 1 + (index // 3) * height,
                    })
                    self.assertTrue(panel.get("description"))
            next_y += 1 + ((len(charts) + 2) // 3) * height

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
        self.assertTrue({
            "mill_cuts_producer_records_sent_total",
            "mill_cuts_producer_send_errors_total",
            "mill_cuts_consumer_records_processed_total",
            "mill_cuts_consumer_processing_errors_total",
        } <= {rule["name"] for rule in cut_metrics["rules"]})

    def test_metrics_that_matter_dashboard_copies_baseline_and_adds_three_outcomes(self):
        second_path = MONITORING / "grafana" / "dashboards" / "metrics-that-matter.json"
        self.assertTrue(second_path.is_file(), f"Missing provisioned dashboard: {second_path}")
        second = json.loads(second_path.read_text())
        baseline = self.dashboard["panels"]
        self.assertEqual(second["panels"][:len(baseline)], baseline)
        self.assertEqual(len(second["panels"]), len(baseline) + 3)
        self.assertEqual(second["templating"]["list"], self.dashboard["templating"]["list"])
        self.assertNotEqual(second["uid"], self.dashboard["uid"])
        self.assertEqual(
            {panel["title"] for panel in second["panels"][len(baseline):]},
            {"Dead-letter topic length", "Unreconciled event rows", "Average entity staleness"},
        )
        outcome_expressions = {
            target["expr"]
            for panel in second["panels"][len(baseline):]
            for target in panel.get("targets", [])
        }
        self.assertTrue({
            "mill_tool_events_dlq_topic_length",
            "mill_tool_events_unreconciled",
            "mill_tool_events_average_staleness_seconds",
        } <= outcome_expressions)

    def test_event_jmx_counters_are_exported_as_distinct_series(self):
        config = yaml.safe_load((MONITORING / "cut-app-jmx.yml").read_text())
        self.assertIn("com.improving.gotogether.events:type=EventMetrics,*", config["includeObjectNames"])
        rules = {rule["name"]: rule for rule in config["rules"]}
        expected = {
            "mill_tool_events_producer_records_sent_total": "client_id",
            "mill_tool_events_producer_send_errors_total": "client_id",
            "mill_tool_events_consumer_records_processed_total": "group_id",
            "mill_tool_events_consumer_processing_errors_total": "group_id",
        }
        self.assertTrue(set(expected) <= set(rules))
        for metric, label in expected.items():
            with self.subTest(metric=metric):
                self.assertIn(label, rules[metric]["labels"])

    def test_event_metrics_scrapes_and_outcome_panels_use_independent_sources(self):
        jobs = {job["job_name"]: job for job in self.prometheus["scrape_configs"]}
        self.assertEqual(jobs["mill-tool-events-producer"]["static_configs"][0]["targets"], ["mill-tool-events-producer:9406"])
        self.assertEqual(jobs["mill-tool-events-consumer"]["static_configs"][0]["targets"], ["mill-tool-events-consumer:9406"])
        self.assertEqual(jobs["mill-tool-events-outcomes"]["static_configs"][0]["targets"], ["mill-tool-events-outcomes:9408"])
        lag_job = jobs["mill-cuts-group-lag"]["static_configs"][0]["targets"]
        self.assertEqual(lag_job, ["mill-cuts-lag-exporter:9407"])

        second = json.loads((MONITORING / "grafana" / "dashboards" / "metrics-that-matter.json").read_text())
        panels = {panel["title"]: panel for panel in second["panels"]}
        dlq = panels["Dead-letter topic length"]
        unreconciled = panels["Unreconciled event rows"]
        staleness = panels["Average entity staleness"]
        self.assertIn("mill_tool_events_dlq_topic_length", {target["expr"] for target in dlq["targets"]})
        self.assertIn("mill_tool_events_unreconciled", {target["expr"] for target in unreconciled["targets"]})
        self.assertIn("mill_tool_events_average_staleness_seconds", {target["expr"] for target in staleness["targets"]})
        self.assertEqual(staleness["fieldConfig"]["defaults"]["unit"], "s")
        self.assertEqual(staleness["fieldConfig"]["defaults"]["noValue"], "No data")

    def test_streamlens_cluster_is_preregistered_readonly_and_dashboards_untouched(self):
        clusters_path = MONITORING / "streamlens" / "clusters.json"
        self.assertTrue(clusters_path.is_file(), f"Missing StreamLens cluster config: {clusters_path}")
        clusters = json.loads(clusters_path.read_text())["clusters"]
        cluster = next(entry for entry in clusters if entry.get("name") == "local")
        self.assertEqual(cluster["bootstrapServers"], "broker:29092")
        self.assertEqual(cluster["schemaRegistryUrl"], "http://schema-registry:8081")
        self.assertEqual(cluster["prometheusUrl"], "http://prometheus:9090")
        self.assertEqual(cluster["securityProtocol"], "PLAINTEXT")
        self.assertFalse(cluster["enableKafkaEventProduceFromUi"])
        self.assertNotIn("jmxHost", cluster)
        self.assertNotIn("jmxPort", cluster)

        config = yaml.safe_load((MONITORING / "cut-app-jmx.yml").read_text())
        self.assertIn("kafka.producer:type=producer-topic-metrics,*", config["includeObjectNames"])
        rule = next(
            rule for rule in config["rules"]
            if rule["name"] == "kafka_producer_topic_metrics_record_send_total"
        )
        self.assertEqual(rule["type"], "COUNTER")
        self.assertEqual(rule["labels"], {"client_id": "$1", "topic": "$2"})

        status = subprocess.run(
            ["git", "-C", str(ROOT), "status", "--porcelain", "--", "monitoring/grafana/dashboards"],
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(status.stdout.strip(), "", f"Grafana dashboards changed: {status.stdout}")

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

    def test_dashboard_exposes_missing_telemetry_and_uses_relevant_exported_series(self):
        expressions = {
            target["expr"]
            for panel in self.dashboard["panels"]
            for target in panel.get("targets", [])
        }
        not_on_dashboard = {
            "kafka_server_broker_topic_metrics_bytes_in_total",
            "kafka_server_broker_topic_metrics_bytes_out_total",
            "kafka_producer_topic_metrics_record_send_total",
        }
        for filename in ("kafka-jmx.yml", "schema-registry-jmx.yml", "cut-app-jmx.yml"):
            rules = yaml.safe_load((MONITORING / filename).read_text())["rules"]
            for rule in rules:
                with self.subTest(metric=rule["name"]):
                    present = any(rule["name"] in expression for expression in expressions)
                    self.assertEqual(present, rule["name"] not in not_on_dashboard)

        traffic = next(panel for panel in self.dashboard["panels"] if panel["title"] == "Kafka throughput")
        self.assertEqual(traffic["fieldConfig"]["defaults"]["noValue"], "No data")
        self.assertIn("zero", traffic["description"].lower())
        self.assertIn("missing", traffic["description"].lower())

    def test_dashboard_documentation_matches_provisioned_selectors_health_and_outcomes(self):
        readme = (ROOT / "README.md").read_text()
        for variable in self.dashboard["templating"]["list"]:
            with self.subTest(selector=variable["label"]):
                self.assertIn(variable["label"], readme)
        for row in (panel for panel in self.dashboard["panels"] if panel["type"] == "row"):
            with self.subTest(section=row["title"]):
                self.assertIn(row["title"], readme)
        for phrase in (
            "defaults to All",
            "Kafka throughput",
            "not broker byte rates",
            "only the topic-labeled lag panel",
            "Healthy (green)",
            "Unhealthy (red)",
            "Unavailable (gray)",
            "Dead-letter topic length",
            "Unreconciled event rows",
            "Average entity staleness",
            "stat panels",
            "seconds",
        ):
            with self.subTest(documentation=phrase):
                self.assertIn(phrase.lower(), readme.lower())

    def test_event_runbook_documents_topics_tables_metrics_and_both_dashboards(self):
        readme = (ROOT / "README.md").read_text()
        for value in (
            "mill-tool-events-source",
            "mill-tool-events-dlq",
            "mill-tool-events-consumer",
            "mill_tool_event_reconciliation",
            "mill_tool_event_state",
            "mill_tool_events_dlq_topic_length",
            "mill_tool_events_unreconciled",
            "mill_tool_events_average_staleness_seconds",
            "metrics-that-matter",
            "error header",
            "retention",
            "processed = false",
        ):
            with self.subTest(value=value):
                self.assertIn(value, readme)

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
