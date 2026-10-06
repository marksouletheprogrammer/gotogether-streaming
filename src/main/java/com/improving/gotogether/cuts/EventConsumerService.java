package com.improving.gotogether.cuts;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

public final class EventConsumerService {
    private static final Logger LOGGER = Logger.getLogger(EventConsumerService.class.getName());
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    private EventConsumerService() {
    }

    public static void runFromEnvironment() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> RUNNING.set(false)));
        String groupId = requiredEnvironment("CONSUMER_GROUP_ID");
        double dlqProbability = probability("EVENT_FAILURE_PROBABILITY", 0.2);
        double silentDropProbability = probability("EVENT_SILENT_DROP_PROBABILITY", 0.05);
        try (EventMetrics metrics = EventMetrics.consumer(groupId)) {
            while (RUNNING.get()) {
                try {
                    runConsumerIteration(metrics, dlqProbability, silentDropProbability);
                } catch (RuntimeException exception) {
                    metrics.recordProcessingFailure();
                    if (RUNNING.get()) {
                        LOGGER.log(Level.SEVERE, "Technician event consumer iteration failed; retrying", exception);
                        pauseBeforeRetry();
                    }
                }
            }
        }
    }

    private static void runConsumerIteration(EventMetrics metrics, double dlqProbability, double silentDropProbability) {
        String topic = requiredEnvironment("EVENT_TOPIC");
        String deadLetterTopic = requiredEnvironment("EVENT_DLQ_TOPIC");
        String consumerGroup = requiredEnvironment("CONSUMER_GROUP_ID");
        String clientId = requiredEnvironment("KAFKA_CLIENT_ID");
        String registryUrl = requiredEnvironment("SCHEMA_REGISTRY_URL");
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        String jdbcUrl = System.getenv().getOrDefault("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/gotogether");
        String databaseUser = System.getenv().getOrDefault("POSTGRES_USER", "gotogether");
        String databasePassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "");
        Properties consumerProperties = CutConsumerConfig.create(brokers, consumerGroup, clientId, registryUrl);
        Properties producerProperties = EventProducerConfig.create(brokers, clientId + "-dlq", registryUrl);

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProperties);
             KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProperties)) {
            EventStateRepository repository = new PostgresEventStateRepository(jdbcUrl, databaseUser, databasePassword);
            KafkaEventDeadLetterWriter deadLetterWriter = new KafkaEventDeadLetterWriter(deadLetterTopic, record -> {
                try {
                    producer.send(record).get();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while publishing event to the dead-letter topic", exception);
                } catch (ExecutionException exception) {
                    throw new IllegalStateException("Could not publish event to the dead-letter topic", exception.getCause());
                }
            });
            EventConsumer processor = new EventConsumer(
                repository,
                deadLetterWriter,
                dlqProbability,
                silentDropProbability,
                () -> ThreadLocalRandom.current().nextDouble()
            );
            consumer.subscribe(List.of(topic));
            while (RUNNING.get()) {
                for (ConsumerRecord<String, GenericRecord> record : consumer.poll(Duration.ofMillis(500))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    EventConsumer.Outcome outcome = processor.process(record.value(), () -> consumer.commitSync(Map.of(
                        partition,
                        new OffsetAndMetadata(record.offset() + 1)
                    )));
                    metrics.recordProcessed();
                    if (outcome == EventConsumer.Outcome.DEAD_LETTERED) {
                        metrics.recordProcessingFailure();
                        LOGGER.info(() -> "Dead-lettered " + record.key() + " from " + topic);
                    } else if (outcome == EventConsumer.Outcome.SILENTLY_DROPPED) {
                        LOGGER.info(() -> "Silently dropped " + record.key() + " from " + topic);
                    } else {
                        LOGGER.info(() -> "Applied " + record.key() + " from " + topic);
                    }
                }
            }
        }
    }

    private static String requiredEnvironment(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable " + key);
        }
        return value;
    }

    private static double probability(String key, double defaultValue) {
        double value = Double.parseDouble(System.getenv().getOrDefault(key, Double.toString(defaultValue)));
        if (!Double.isFinite(value) || value < 0 || value > 1) {
            throw new IllegalArgumentException(key + " must be between zero and one");
        }
        return value;
    }

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            RUNNING.set(false);
        }
    }
}
