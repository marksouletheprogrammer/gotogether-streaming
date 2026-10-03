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
- The base project is extensible and MAY include application producers, consumers, databases, and other services required by its features. Do not treat "base project" as a limit on adding functionality; include required services in the ordinary Docker Compose startup.
- This project should be monitorable. There should be a grafana dashboard reachable by machine's localhost. Grafana should be hosted in a docker container.
- The Grafana instance should have a dashboard pre-installed that follows industry standard metrics/panels for a Kafka deployment.
- Grafana dashboard should display metrics from the locally this project's kafka cluster and schema registry.
- Use a more recent version of Kafka that uses KRaft instead of zookeeper.

### Exactly once semantics
- Create two end to end pipelines based on Kafka.
- The first pipeline should be an exactly once pipeline. With an kafka idempotent producer and a kafka transactional consumer.
- The second pipeline should be an effective once pipeline. With an kafka at-least-once producer and a kafka idempotent consumer.
- Both pipeline's consumers should write data to a local postgres instance. But in different tables.
- The data schema should come from @DATA.md. Pick an approprite entity for this pipeline.
- Both producers will produce the exactly same series of synthetic data from the schema chosen. The idea is to compare both pipelines side by side. Therefore they will need to produce exactly the same data at the same rate.
- We do not need a lot of data so keep the message rate relatively low.
- Postgres should run in docker and connectable from other apps in docker and from localhost (for debugging and demonstration).
- Both consumers should have their own consumer groups.
- Both producers should have their own topics and client IDs.
- Naming of all objects should be based on the chosen schema entity/domain. Do not name based off of this spec's name.
- Monitoring is important. Consumer groups should get their metrics displayed in grafana. Including, but not limited to: lag, throughput, error rates, etc. Producer metrics should also be displayed in grafana.

### Metrics that matter
- Add a new pipeline with a new producer, topic, and consumer. This one will target a different entity from DATA.md. Do not choose the same entity as the previous pipelines.
- This new pipeline should also have monitoring in grafana.
- The producer will write a row to a postgres table for each message it produces. This is the reconciliation table.
- The consumer will mark each row in the reconciliation table as processed when it consumes the message. It does this in the same transaction that is uses to write the data to the target table.
- The consumer for this new pipeline can randomly fail to process message. When this happens it writes a message to a dead letter queue with the error in the headers of the message.
- Create a second grafana dashboard that is an exact copy of the first one with some exceptions that will be detailed next.
- The second dashboard should have a panel for the dead letter queue depth.
- The second dashboard should have a panel for the number of messages that are NOT reconcilled. 
- The second dashboard should have a panel the average staleness of messages. This means that a message was updated at 5:00pm, and the current time is 5:05pm, so the staleness is 5 minutes. The average is calculated across all unique entites from the target table.
- This new pipeline should start up and start running when the rest of the system starts up. It will run in parallel with all the other pipelines. 
- The reason why we have 2 separate dashboards is that we want to show a dashboard with the "typical" metrics vs the dashboard that actually matter.
- All messages for new pipelines should be in AVRO. 
- The synthetic data should sometimes use the same entities so that the entity is updates occasionally.
- The synthetic data should sometimes use different entities so that the entity is not updated.
- The target table should have an updated_at timestamp column.

## Constraints and Non-Goals
- No authentication.

## Scope Guidance for the AI
- The user will request one feature at a time (e.g., `Base Project, include greenfield scaffolding`).
- Generate the OpenSpec proposal/spec/design/tasks artifacts for only the requested feature.
- Do not edit or rewrite `DATA.md` unless the user explicitly asks for changes to that file.

## Follow up for future enhancements
- The grafana dashboard should have separate panels for throughput, lag, and error rates and then have a dropdown to switch between topics, consumer groups, and producers. The "Transactional cut producer throughput and errors", "At-least-once cut producer throughput and errors", "At-least-once cut producer throughput and errors", and "Transactional cut consumer lag, throughput and errors" do not really make sense.
- The production rate of messages can be cranked up a bit.
- Topic naming is weird. The names do not make sense.