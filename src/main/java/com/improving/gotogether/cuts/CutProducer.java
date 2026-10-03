package com.improving.gotogether.cuts;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class CutProducer {
    private static final Logger LOGGER = Logger.getLogger(CutProducer.class.getName());

    private CutProducer() {
    }

    public static void run(boolean idempotent) {
        String topic = requiredEnvironment("CUT_TOPIC");
        String clientId = requiredEnvironment("KAFKA_CLIENT_ID");
        String registryUrl = requiredEnvironment("SCHEMA_REGISTRY_URL");
        int recordCount = positiveInteger("CUT_RECORD_COUNT", 30);
        long intervalMillis = positiveLong("CUT_INTERVAL_MS", 2_000);
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        Properties properties = idempotent
            ? CutProducerConfig.transactionalProducer(brokers, clientId, registryUrl)
            : CutProducerConfig.atLeastOnceProducer(brokers, clientId, registryUrl);
        CutRecordGenerator generator = new CutRecordGenerator();

        try (CutMetrics metrics = CutMetrics.producer(clientId);
             KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties)) {
            for (int cutIndex = 1; cutIndex <= recordCount; cutIndex++) {
                GenericRecord cut = generator.generate(cutIndex);
                String eventId = cut.get("event_id").toString();
                try {
                    producer.send(new ProducerRecord<>(topic, eventId, cut)).get();
                    metrics.recordSend();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    metrics.recordSendFailure();
                    throw new IllegalStateException("Interrupted while publishing cut " + eventId, exception);
                } catch (ExecutionException exception) {
                    metrics.recordSendFailure();
                    throw new IllegalStateException("Could not publish cut " + eventId, exception.getCause());
                } catch (RuntimeException exception) {
                    metrics.recordSendFailure();
                    throw exception;
                }
                LOGGER.info(() -> "Published " + eventId + " to " + topic);
                if (cutIndex < recordCount) {
                    sleep(intervalMillis);
                }
            }
            producer.flush();
            LOGGER.info(() -> "Finished publishing " + recordCount + " cuts to " + topic);
            keepAliveUntilShutdown();
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
            throw new IllegalStateException("Interrupted while pacing cut generation", exception);
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
