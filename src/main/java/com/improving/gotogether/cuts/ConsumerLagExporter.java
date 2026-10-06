package com.improving.gotogether.cuts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public final class ConsumerLagExporter {
    private static final Logger LOGGER = Logger.getLogger(ConsumerLagExporter.class.getName());
    private static final String DLQ_TOPIC = "mill-tool-events-dlq";
    static final List<LagTarget> TARGETS = List.of(
        new LagTarget("mill-cuts-transactional-consumer", "mill-cuts-transactional-source"),
        new LagTarget("mill-cuts-idempotent-consumer", "mill-cuts-atleastonce-source"),
        new LagTarget("mill-tool-events-consumer", "mill-tool-events-source")
    );

    private final AdminClient admin;
    private final int port;
    private final AtomicReference<String> exposition = new AtomicReference<>(renderMetrics(List.of(), false, null, false));

    private ConsumerLagExporter(String brokers, int port) {
        this.port = port;
        admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000
        ));
    }

    public static void runFromEnvironment() {
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        int port = Integer.parseInt(System.getenv().getOrDefault("LAG_EXPORTER_PORT", "9407"));
        new ConsumerLagExporter(brokers, port).run();
    }

    private void run() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
            server.createContext("/metrics", this::serveMetrics);
            ExecutorService httpExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "cut-lag-exporter-http");
                thread.setDaemon(true);
                return thread;
            });
            server.setExecutor(httpExecutor);
            server.start();
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(this::refresh, 0, 5, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdownNow();
                httpExecutor.shutdownNow();
                server.stop(1);
                admin.close(Duration.ofSeconds(2));
            }));
            new CountDownLatch(1).await();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not start consumer lag exporter on port " + port, exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void refresh() {
        List<LagSample> lagSamples = List.of();
        boolean lagSuccess = false;
        try {
            lagSamples = collectLagSamples();
            lagSuccess = true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Could not refresh consumer group lag", exception);
        } catch (ExecutionException | TimeoutException | RuntimeException exception) {
            LOGGER.log(Level.WARNING, "Could not refresh consumer group lag", exception);
        }

        Long dlqLength = null;
        boolean dlqSuccess = false;
        try {
            dlqLength = readDlqTopicLength();
            dlqSuccess = true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Could not refresh event DLQ topic length", exception);
        } catch (ExecutionException | TimeoutException | RuntimeException exception) {
            LOGGER.log(Level.WARNING, "Could not refresh event DLQ topic length", exception);
        }
        exposition.set(renderMetrics(lagSamples, lagSuccess, dlqLength, dlqSuccess));
    }

    private List<LagSample> collectLagSamples() throws InterruptedException, ExecutionException, TimeoutException {
        List<LagSample> samples = new ArrayList<>();
        for (LagTarget target : TARGETS) {
            TopicPartition partition = new TopicPartition(target.topic(), 0);
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets(target.groupId())
                .partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS)
                .get(partition);
            Long currentOffset = committed == null ? null : committed.offset();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = admin.listOffsets(
                Map.of(partition, OffsetSpec.latest())
            ).all().get(5, TimeUnit.SECONDS);
            samples.add(new LagSample(
                target.groupId(),
                target.topic(),
                partition.partition(),
                calculateLag(currentOffset, endOffsets.get(partition).offset())
            ));
        }
        return samples;
    }

    private long readDlqTopicLength() throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, TopicDescription> descriptions = admin.describeTopics(List.of(DLQ_TOPIC))
            .allTopicNames()
            .get(5, TimeUnit.SECONDS);
        TopicDescription description = descriptions.get(DLQ_TOPIC);
        if (description == null) {
            throw new IllegalStateException("Missing Kafka topic " + DLQ_TOPIC);
        }
        List<TopicPartition> partitions = description.partitions().stream()
            .map(partition -> new TopicPartition(DLQ_TOPIC, partition.partition()))
            .toList();
        Map<TopicPartition, OffsetSpec> earliestSpecs = new LinkedHashMap<>();
        Map<TopicPartition, OffsetSpec> latestSpecs = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) {
            earliestSpecs.put(partition, OffsetSpec.earliest());
            latestSpecs.put(partition, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest = admin.listOffsets(earliestSpecs)
            .all().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = admin.listOffsets(latestSpecs)
            .all().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
        Map<TopicPartition, Long> latestOffsets = new HashMap<>();
        earliest.forEach((partition, offset) -> earliestOffsets.put(partition, offset.offset()));
        latest.forEach((partition, offset) -> latestOffsets.put(partition, offset.offset()));
        return calculateTopicLength(earliestOffsets, latestOffsets);
    }

    private void serveMetrics(HttpExchange exchange) throws IOException {
        byte[] body = exposition.get().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (var stream = exchange.getResponseBody()) {
            stream.write(body);
        }
    }

    static long calculateLag(Long currentOffset, long endOffset) {
        return Math.max(endOffset - (currentOffset == null ? 0 : currentOffset), 0);
    }

    static long calculateTopicLength(Map<TopicPartition, Long> earliest, Map<TopicPartition, Long> latest) {
        if (!earliest.keySet().equals(latest.keySet())) {
            throw new IllegalArgumentException("DLQ earliest/latest offsets must cover the same partitions");
        }
        return earliest.entrySet().stream()
            .mapToLong(entry -> Math.max(latest.get(entry.getKey()) - entry.getValue(), 0))
            .sum();
    }

    static String renderMetrics(List<LagSample> lagSamples, boolean lagSuccess, Long dlqLength, boolean dlqSuccess) {
        StringBuilder output = new StringBuilder();
        if (lagSuccess) {
            for (LagSample sample : lagSamples) {
                output.append("mill_cuts_consumer_group_lag{group_id=\"")
                    .append(escape(sample.groupId()))
                    .append("\",topic=\"")
                    .append(escape(sample.topic()))
                    .append("\",partition=\"")
                    .append(sample.partition())
                    .append("\"} ")
                    .append(sample.lag())
                    .append('\n');
            }
        }
        output.append("mill_cuts_consumer_lag_collection_success ").append(lagSuccess ? 1 : 0).append('\n');
        output.append("mill_tool_events_dlq_collection_success ").append(dlqSuccess ? 1 : 0).append('\n');
        if (dlqSuccess && dlqLength != null) {
            output.append("mill_tool_events_dlq_topic_length ").append(dlqLength).append('\n');
        }
        return output.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    record LagTarget(String groupId, String topic) {
    }

    record LagSample(String groupId, String topic, int partition, long lag) {
    }
}
