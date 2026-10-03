FROM python:3.13.7-slim AS jmx-agent
COPY scripts/download_jmx_exporter.py /tmp/download_jmx_exporter.py
RUN python /tmp/download_jmx_exporter.py

FROM maven:3.9.10-eclipse-temurin-21-alpine AS build

WORKDIR /workspace
COPY pom.xml .
RUN mvn --batch-mode --no-transfer-progress dependency:go-offline
COPY src ./src
RUN mvn --batch-mode --no-transfer-progress -DskipTests package

FROM eclipse-temurin:21.0.7_6-jre-alpine

WORKDIR /app
COPY --from=jmx-agent /opt/jmx_prometheus_javaagent-1.6.0.jar /usr/share/java/jmx_prometheus_javaagent-1.6.0.jar
COPY --from=build /workspace/target/mill-cuts-pipelines-1.0.0.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
