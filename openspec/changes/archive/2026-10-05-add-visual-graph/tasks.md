# Tasks

## 1. Tests first

- [x] 1.1 Extend `tests/test_compose_contract.py` with a `streamlens` service test (git build pin, `127.0.0.1:5000:5000`, `CLUSTERS_JSON`, read-only config mount, healthcheck, depends_on, default-started with no profile); verify it fails before implementation (spec: Visualization service on default startup)
- [x] 1.2 Extend `tests/test_monitoring_contract.py` to assert the cluster file contents (broker, registry, prometheus URLs, produce-from-UI disabled), the JMX include/rule producing `kafka_producer_topic_metrics_record_send_total` with `client_id` and `topic` labels, and that Grafana dashboard JSON files are unchanged; verify it fails before implementation (specs: Producer discoverability, Additive to existing tools)

## 2. Producer discoverability

- [x] 2.1 Add the `kafka.producer:type=producer-topic-metrics,*` include and counter rule to `monitoring/cut-app-jmx.yml`; verify the 1.2 JMX assertions pass (spec: Per-producer, per-topic series)

## 3. StreamLens service

- [x] 3.1 Create `monitoring/streamlens/clusters.json` for the project's cluster; verify the 1.2 cluster-file assertions pass (spec: Cluster is pre-registered, Read-only against Kafka)
- [x] 3.2 Add the `streamlens` service to `compose.yaml` per design decisions 1, 2, 4, 5; verify the 1.1 test passes and `docker compose config` is valid (spec: Start the full stack)

## 4. Documentation

- [x] 4.1 Update `README.md`: add port 5000 to prerequisites, add StreamLens to the service table, and add step-by-step instructions to open the graph and the expected producers, topics, schemas, and consumer groups; verify the README names `http://localhost:5000` (spec: Find StreamLens instructions)

## 5. Verification (requires user permission to run Docker)

- [x] 5.1 Run the full Python test suite and verify it passes, including pre-existing tests (spec: Existing tools unchanged)
- [x] 5.2 With the user's approval, start the stack and verify `curl http://localhost:5000/health` succeeds, the Prometheus query for `kafka_producer_topic_metrics_record_send_total` returns `client_id`/`topic` labelled series, and the StreamLens topology shows all three pipelines, DLQ, committed topic, and schemas (specs: End-to-end pipeline topology, Producers persist after finite series)
- [x] 5.3 Confirm AKHQ and both Grafana dashboards still load unchanged (spec: Existing tools unchanged)
