# Tasks

## 1. Canonical schema and test scaffolding

- [x] 1.1 Add a pinned Draft 2020-12 test dependency and a failing static check that extracts the unique schema and three example records from `DATA.md`, checks date-time formats, rejects missing readings/malformed timestamps, and detects asset drift; verify it fails when the schema asset is missing (Scenario: Validate the canonical examples).
- [x] 1.2 Add a deterministic schema extraction command and checked-in `schemas/cnc-demo-records.schema.json` from `DATA.md` without modifying the source document; verify the contract checks now pass and the generated schema matches the canonical block (Scenario: Inspect initial topic and schema).
- [x] 1.3 Refactor the extractor and contract tests to share one parsing path and fail clearly on missing/duplicate schema blocks; verify the same checks still pass (Scenario: Verify a new setup).

## 2. Kafka, registry, and local admin UI

- [x] 2.1 Add a failing Compose configuration/port assertion for single-node KRaft, internal and loopback broker listeners, Compose-managed demo storage, no ZooKeeper, and no producer/consumer/database service; verify it detects a missing stack before implementation (Scenarios: Start from a clean checkout; Scope stays limited to the base platform). Obtain user permission before any Docker command.
- [x] 2.2 Add pinned, matching KRaft Kafka and schema registry services with health checks, internal broker endpoint, loopback host endpoint, low-footprint single-node settings, and volumes scoped to this Compose project; verify the Compose assertions pass and, with permission, both services reach healthy status (Scenarios: Start from a clean checkout; Access cluster from host and containers).
- [x] 2.3 Add pinned AKHQ with Kafka and registry connections and a localhost-only port; verify a host request loads AKHQ and lists the cluster after startup (Scenario: Inspect Kafka with AKHQ). Obtain permission before Docker startup.
- [x] 2.4 Refactor Compose readiness dependencies and shared settings to remove duplication without changing ports, health, or service scope; verify `docker compose config` and cluster metadata from host and container still pass (Scenario: Access cluster from host and containers). Obtain permission before Docker commands.

## 3. Fresh-start sample setup

- [x] 3.1 Extend the smoke checks to fail on missing `cnc-demo-records`, nonempty offsets, or absent/wrong `cnc-demo-records-value` schema; verify failure against a stack before sample setup completes (Scenario: Inspect initial topic and schema). Obtain permission before Docker checks.
- [x] 3.2 Add a short-lived, health-gated setup job that creates one-partition `cnc-demo-records` and registers the checked-in JSON Schema once per fresh Compose deployment; verify topic offsets are zero and registry returns the canonical Draft 2020-12 schema (Scenario: Inspect initial topic and schema). Obtain permission before Docker startup.
- [x] 3.3 Refactor setup readiness and error reporting so repeated `docker compose up -d` reuses the existing topic and identical canonical schema without another version, while a conflicting schema or unsuccessful setup exits nonzero; verify repeated startup preserves topic offsets and schema version, and the smoke check reports missing resources (Scenarios: Repeat startup without resetting demo state; Reject a conflicting persisted schema; Verify a new setup). Obtain permission before Docker checks.

## 4. Live monitoring

- [x] 4.1 Add a failing monitoring check for separate broker/registry Prometheus targets, required metric series, and a provisioned Grafana dashboard/datasource; verify it reports missing targets/panels before implementation (Scenarios: Open an active dashboard; Broker or registry becomes unreachable). Obtain permission before Docker checks.
- [x] 4.2 Add pinned, checksum-verified JMX exporter javaagents and bounded broker/registry metric rules; configure pinned Prometheus scrapes and storage, and verify both `up` targets and required live service metrics are present (Scenario: Open an active dashboard). Obtain permission before Docker checks.
- [x] 4.3 Add pinned Grafana with anonymous Viewer on localhost and provision Prometheus plus dashboard panels for broker health, throughput, request/error or latency, and registry availability/requests/errors; verify dashboard loads without login and each panel query maps to live series (Scenario: Open an active dashboard). Obtain permission before Docker checks.
- [x] 4.4 Refactor metric rules/panels to eliminate unused series and explicitly distinguish zero traffic from missing telemetry; verify a stopped scrape target reports `up=0` while the unaffected service remains healthy (Scenario: Broker or registry becomes unreachable). Obtain permission before Docker commands.

## 5. End-to-end runbook and acceptance

- [x] 5.1 Add a concise `README.md` with Docker/Python test prerequisites, exact static-check and Compose startup/verification/teardown commands, localhost URLs, broker and registry endpoints, expected empty topic/schema, and a prominent warning that `docker compose down --volumes` permanently discards all demo data; verify each instruction agrees with the checked-in configuration (Scenarios: Verify a new setup; Stop the stack).
- [x] 5.2 From a clean checkout run schema checks, `docker compose config`, start the full stack, run smoke checks, and confirm AKHQ topic visibility, registry schema identity, dashboard live broker/registry series, and absence of producers/consumers; verify all reported checks pass (Scenarios: Start from a clean checkout; Verify a new setup; Scope stays limited to the base platform). Obtain user permission before any Docker command.
- [x] 5.3 After explicit user approval for deleting this stack's volumes, run `docker compose down --volumes`, restart, and rerun smoke checks; verify previous data/metrics history is gone and a fresh empty topic and schema exist (Scenarios: Stop the stack; Teardown discards demo state). Obtain separate approval for this destructive command even if earlier Docker checks were authorized.
