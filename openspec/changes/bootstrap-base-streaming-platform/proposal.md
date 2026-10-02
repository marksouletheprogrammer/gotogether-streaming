# Proposal

## Why

The repository currently contains product requirements and a canonical demo data contract, but no runnable infrastructure. A local, observable Kafka foundation is needed before later predictive-maintenance producers, consumers, and Java streaming applications can be added and demonstrated.

## What Changes

- Add disposable Docker Compose scaffolding for a lightweight, single-node Kafka KRaft deployment, a local schema registry, AKHQ, and a pre-provisioned Grafana monitoring stack; teardown discards local demo state.
- On each fresh startup, create an empty sample CNC demo records topic and register the canonical JSON Schema from `DATA.md`; repeated startup preserves an existing topic and matching schema version, and rejects a conflicting schema without replacing it. Do not generate or publish sample records.
- Expose AKHQ and Grafana on the host's localhost and connect services on an internal Compose network; include broker and registry health/metrics collection and a Kafka/registry dashboard.
- Add configuration, smoke/contract verification, and concise startup, verification, and teardown instructions so the stack can be run end-to-end from a clean checkout.
- Do not add any producers, consumers, Kafka Streams jobs, synthetic data generation, authentication, or unused Java/Postgres services in this base slice; those belong to future feature changes.

## Capabilities

### New Capabilities

- `base-streaming-platform`: Disposable Compose lifecycle, Kafka KRaft connectivity, empty CNC demo records topic and canonical schema registration on fresh startup, local AKHQ access, and provisioned broker/registry monitoring.

### Modified Capabilities

None; there are no existing capability specs.

## Impact

New Compose, service/monitoring configuration, schema asset and bootstrap/verification tooling, plus brief operator documentation. The only existing source of data semantics is `DATA.md`, which remains unchanged. Requires Docker with Compose and free host ports for the two web UIs; no external services or authentication are required. Later Java/Kafka Streams and Postgres work can attach to this stack but is not deployed here.

Success is a clean `docker compose up -d` followed by healthy services, an empty sample topic and registered compatible schema visible in AKHQ/registry, and a Grafana dashboard showing live broker and registry metrics (including target availability) without manual dashboard import. Verification must exercise these behaviors and clean teardown; running Docker commands during implementation requires user permission.

Rollback is to stop/remove the Compose-managed services and volumes with `docker compose down --volumes` and revert the newly introduced files. Teardown permanently discards Kafka topics, registered schemas, and metrics; do not store irreplaceable data in this demo. Obtain approval before executing this destructive command.
