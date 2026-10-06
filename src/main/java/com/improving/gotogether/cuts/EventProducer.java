package com.improving.gotogether.cuts;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class EventProducer {
    private static final Logger LOGGER = Logger.getLogger(EventProducer.class.getName());

    private final EventReconciliationRepository repository;
    private final Consumer<GenericRecord> writer;
    private final EventRecordGenerator generator;

    EventProducer(EventReconciliationRepository repository, Consumer<GenericRecord> writer) {
        this(repository, writer, new EventRecordGenerator());
    }

    EventProducer(EventReconciliationRepository repository, Consumer<GenericRecord> writer, EventRecordGenerator generator) {
        this.repository = repository;
        this.writer = writer;
        this.generator = generator;
    }

    public void publish(int eventIndex) {
        GenericRecord event = generator.generate(eventIndex);
        String eventId = event.get("event_id").toString();
        repository.ensureEventRow(eventId);
        writer.accept(event);
    }

    public static void runFromEnvironment() {
        String topic = requiredEnvironment("EVENT_TOPIC");
        String clientId = requiredEnvironment("KAFKA_CLIENT_ID");
        String registryUrl = requiredEnvironment("SCHEMA_REGISTRY_URL");
        long intervalMillis = positiveLong("EVENT_INTERVAL_MS", 200);
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        String jdbcUrl = System.getenv().getOrDefault("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/gotogether");
        String databaseUser = System.getenv().getOrDefault("POSTGRES_USER", "gotogether");
        String databasePassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "");
        Properties properties = EventProducerConfig.create(brokers, clientId, registryUrl);

        try (EventMetrics metrics = EventMetrics.producer(clientId);
             KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties)) {
            EventReconciliationRepository repository = new PostgresEventReconciliationRepository(
                jdbcUrl,
                databaseUser,
                databasePassword
            );
            EventProducer eventProducer = new EventProducer(repository, event -> {
                String eventId = event.get("event_id").toString();
                try {
                    producer.send(new ProducerRecord<>(topic, eventId, event)).get();
                    metrics.recordSend();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    metrics.recordSendFailure();
                    throw new IllegalStateException("Interrupted while publishing event " + eventId, exception);
                } catch (ExecutionException exception) {
                    metrics.recordSendFailure();
                    throw new IllegalStateException("Could not publish event " + eventId, exception.getCause());
                } catch (RuntimeException exception) {
                    metrics.recordSendFailure();
                    throw exception;
                }
            });

            // Register shutdown hook for orderly shutdown
            Thread shutdownHook = new Thread(() -> {
                LOGGER.info("Shutdown signal received, stopping event producer");
                Thread.currentThread().interrupt();
            });
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            // Continuous paced publishing until shutdown
            for (long eventIndex = 1; !Thread.currentThread().isInterrupted(); eventIndex++) {
                eventProducer.publish((int) eventIndex);
                LOGGER.info("Published event " + eventIndex + " to " + topic);
                sleep(intervalMillis);
            }
            producer.flush();
            LOGGER.info("Event producer finished");
        }
    }

    private static String requiredEnvironment(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable " + key);
        }
        return value;
    }

    private static int positiveInteger(String key, int defaultValue) {
        int value = Integer.parseInt(System.getenv().getOrDefault(key, Integer.toString(defaultValue)));
        if (value < 1) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static long positiveLong(String key, long defaultValue) {
        long value = Long.parseLong(System.getenv().getOrDefault(key, Long.toString(defaultValue)));
        if (value < 1) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(Duration.ofMillis(millis));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while pacing event generation", exception);
        }
    }

    private static void keepAliveUntilShutdown() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
