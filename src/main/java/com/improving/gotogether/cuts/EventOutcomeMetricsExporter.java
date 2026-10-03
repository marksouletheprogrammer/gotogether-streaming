package com.improving.gotogether.cuts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public final class EventOutcomeMetricsExporter {
    private static final Logger LOGGER = Logger.getLogger(EventOutcomeMetricsExporter.class.getName());
    private static final String UNRECONCILED_QUERY = "SELECT COUNT(*) FROM mill_tool_event_reconciliation WHERE processed = FALSE";
    private static final String UPDATED_AT_QUERY = "SELECT updated_at, CURRENT_TIMESTAMP AS sampled_at FROM mill_tool_event_state";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int port;
    private final AtomicReference<String> exposition = new AtomicReference<>(renderMetrics(0, null, false));

    private EventOutcomeMetricsExporter(String jdbcUrl, String username, String password, int port) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.port = port;
    }

    public static void runFromEnvironment() {
        String jdbcUrl = System.getenv().getOrDefault("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/gotogether");
        String username = System.getenv().getOrDefault("POSTGRES_USER", "gotogether");
        String password = System.getenv().getOrDefault("POSTGRES_PASSWORD", "");
        int port = Integer.parseInt(System.getenv().getOrDefault("EVENT_METRICS_PORT", "9408"));
        new EventOutcomeMetricsExporter(jdbcUrl, username, password, port).run();
    }

    private void run() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
            server.createContext("/metrics", this::serveMetrics);
            ExecutorService httpExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "event-outcome-exporter-http");
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
            }));
            new CountDownLatch(1).await();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not start event outcome exporter on port " + port, exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void refresh() {
        try {
            exposition.set(collectMetrics());
        } catch (SQLException | RuntimeException exception) {
            exposition.set(renderMetrics(0, null, false));
            LOGGER.log(Level.WARNING, "Could not refresh event outcome metrics", exception);
        }
    }

    private String collectMetrics() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            long unreconciled;
            try (PreparedStatement statement = connection.prepareStatement(UNRECONCILED_QUERY);
                 ResultSet result = statement.executeQuery()) {
                result.next();
                unreconciled = result.getLong(1);
            }

            List<Instant> updatedAt = new ArrayList<>();
            Instant sampledAt = null;
            try (PreparedStatement statement = connection.prepareStatement(UPDATED_AT_QUERY);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    Timestamp timestamp = result.getTimestamp("updated_at");
                    Timestamp sampleTimestamp = result.getTimestamp("sampled_at");
                    updatedAt.add(timestamp.toInstant());
                    sampledAt = sampleTimestamp.toInstant();
                }
            }
            Double averageStaleness = sampledAt == null ? null : averageStalenessSeconds(updatedAt, sampledAt);
            return renderMetrics(unreconciled, averageStaleness, true);
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

    static Double averageStalenessSeconds(List<Instant> updatedAt, Instant now) {
        if (updatedAt.isEmpty()) {
            return null;
        }
        return updatedAt.stream()
            .mapToDouble(updated -> Math.max(Duration.between(updated, now).toMillis() / 1_000.0, 0))
            .average()
            .orElseThrow();
    }

    static String renderMetrics(long unreconciled, Double averageStaleness, boolean collectionSuccess) {
        if (!collectionSuccess) {
            return "mill_tool_events_outcome_collection_success 0\n";
        }
        StringBuilder output = new StringBuilder()
            .append("mill_tool_events_unreconciled ").append(unreconciled).append('\n')
            .append("mill_tool_events_outcome_collection_success 1\n");
        if (averageStaleness != null) {
            output.append("mill_tool_events_average_staleness_seconds ").append(averageStaleness).append('\n');
        }
        return output.toString();
    }
}
