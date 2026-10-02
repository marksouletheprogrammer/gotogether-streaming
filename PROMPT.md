# Predictive Maintenance — Product Requirements

Create a local data streaming platform that will demo predictive maintenance and real time data streaming concepts. The entire project should run in Docker and controlled with Docker Compose.

## Tech Stack
- Operations: AKHQ for Kafka admin, Grafana for metrics and health dashboarding.
- Backend: Java
- Streaming: Kafka, Kafka Streams
- Database: Postgres
- Dev runtime: Docker compose running on local machine.
- IoT Data: Generate synthetic events that would be useful for predictive maintence. 


## Features

### Synthetic Data

Synthetic data should be generated from producers in this system. Reference DATA.md for generating all synthetic data. Do not use any other sources for the schema, fields, or semantic inforamtion for any synthetic data. That document is the master document describing data in this project.

### Base project
- Create a full end-to-end streaming architecture based on Apache Kafka. It should be fully deployable with Docker Compose and communicate fully locally.
- The kafka will not need a lot of resources. We will not be pushing a large amount of data through this.
- It should include an UI for managing Kafka. AKHQ should be that UI. It should run locally but users should be able to connect on the host's machines localhost.
- Schema registry SHOULD be included.
- A sample topic and schema SHOULD be included.
- The base project SHOULD NOT include any producers or consumers.
- This project should be monitorable. There should be a grafana dashboard reachable by machine's localhost. Grafana should be hosted in a docker container.
- The Grafana instance should have a dashboard pre-installed that follows industry standard metrics/panels for a Kafka deployment.
- Grafana dashboard should display metrics from the locally this project's kafka cluster and schema registry.
- Use a more recent version of Kafka that uses KRaft instead of zookeeper.

## Constraints and Non-Goals
- No authentication.

## Scope Guidance for the AI
- The user will request one feature at a time (e.g., `Base Project, include greenfield scaffolding`).
- Generate the OpenSpec proposal/spec/design/tasks artifacts for only the requested feature.
- Do not edit or rewrite `DATA.md` unless the user explicitly asks for changes to that file.