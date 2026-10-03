package com.improving.gotogether.cuts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public final class ConsumerLagExporter {
    private static final Logger LOGGER = Logger.getLogger(ConsumerLagExporter.class.getName());
    private static final List<LagTarget> TARGETS = List.of(
        new LagTarget("mill-cuts-transactional-consumer", "mill-cuts-transactional-source"),
        new LagTarget("mill-cuts-idempotent-consumer", "mill-cuts-replay-source")
    );

    private final AdminClient admin;
    private final int port;
    private final AtomicReference<String> exposition = new AtomicReference<>(
        "mill_cuts_consumer_lag_collection_success 0\n"
    );

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
        try {
            StringBuilder output = new StringBuilder();
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
                long endOffset = endOffsets.get(partition).offset();
                output.append("mill_cuts_consumer_group_lag{group_id=\"")
                    .append(escape(target.groupId()))
                    .append("\",topic=\"")
                    .append(escape(target.topic()))
                    .append("\",partition=\"0\"} ")
                    .append(calculateLag(currentOffset, endOffset))
                    .append('\n');
            }
            output.append("mill_cuts_consumer_lag_collection_success 1\n");
            exposition.set(output.toString());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException | RuntimeException exception) {
            exposition.set("mill_cuts_consumer_lag_collection_success 0\n");
            LOGGER.log(Level.WARNING, "Could not refresh consumer group lag", exception);
        }
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

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private record LagTarget(String groupId, String topic) {
    }
}
