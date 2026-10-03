package com.improving.gotogether.cuts;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public final class TransactionalCutService {
    private static final Logger LOGGER = Logger.getLogger(TransactionalCutService.class.getName());
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    private TransactionalCutService() {
    }

    public static void runFromEnvironment() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> RUNNING.set(false)));
        String groupId = requiredEnvironment("CONSUMER_GROUP_ID");
        try (CutMetrics metrics = CutMetrics.consumer(groupId)) {
            while (RUNNING.get()) {
                try {
                    runConsumerIteration(metrics);
                } catch (RuntimeException exception) {
                    metrics.recordProcessingFailure();
                    if (RUNNING.get()) {
                        LOGGER.log(Level.SEVERE, "Transactional cut consumer iteration failed; retrying", exception);
                        pauseBeforeRetry();
                    }
                }
            }
        }
    }

    private static void runConsumerIteration(CutMetrics metrics) {
        String topic = requiredEnvironment("CUT_TOPIC");
        String consumerGroup = requiredEnvironment("CONSUMER_GROUP_ID");
        String clientId = requiredEnvironment("KAFKA_CLIENT_ID");
        String registryUrl = requiredEnvironment("SCHEMA_REGISTRY_URL");
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        String jdbcUrl = System.getenv().getOrDefault("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/gotogether");
        String databaseUser = System.getenv().getOrDefault("POSTGRES_USER", "gotogether");
        String databasePassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "");
        String transactionalId = requiredEnvironment("KAFKA_TRANSACTIONAL_ID");
        String processedTopic = requiredEnvironment("PROCESSED_TOPIC");

        Properties consumerProperties = CutConsumerConfig.create(brokers, consumerGroup, clientId, registryUrl);
        Properties producerProperties = CutProducerConfig.transactionalProducer(
            brokers,
            clientId + "-output",
            registryUrl
        );
        try (
            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProperties);
            TransactionalKafkaWriter kafka = new TransactionalKafkaWriter(producerProperties, transactionalId)
        ) {
            CutRecordRepository repository = new PostgresCutRepository(
                jdbcUrl,
                databaseUser,
                databasePassword,
                PostgresCutRepository.Table.TRANSACTIONAL
            );
            TransactionalCutConsumer processor = new TransactionalCutConsumer(processedTopic, kafka, repository);
            consumer.subscribe(List.of(topic));
            while (RUNNING.get()) {
                for (ConsumerRecord<String, GenericRecord> record : consumer.poll(Duration.ofMillis(500))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    processor.process(
                        record.value(),
                        partition,
                        new OffsetAndMetadata(record.offset() + 1),
                        consumer.groupMetadata()
                    );
                    metrics.recordProcessed();
                    LOGGER.info(() -> "Processed " + record.key() + " from " + topic);
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

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            RUNNING.set(false);
        }
    }
}
