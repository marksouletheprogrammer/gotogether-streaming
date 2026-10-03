package com.improving.gotogether.cuts;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public final class TransactionalKafkaWriter implements KafkaTransaction, AutoCloseable {
    private final KafkaProducer<String, GenericRecord> producer;

    public TransactionalKafkaWriter(Properties properties, String transactionalId) {
        Properties transactionalProperties = new Properties();
        transactionalProperties.putAll(properties);
        transactionalProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producer = new KafkaProducer<>(transactionalProperties);
        producer.initTransactions();
    }

    @Override
    public void begin() {
        producer.beginTransaction();
    }

    @Override
    public void send(String topic, GenericRecord record) {
        try {
            producer.send(new ProducerRecord<>(topic, record.get("event_id").toString(), record)).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while producing transactional cut", exception);
        } catch (ExecutionException exception) {
            throw new IllegalStateException("Could not produce transactional cut", exception.getCause());
        }
    }

    @Override
    public void sendOffsets(TopicPartition partition, OffsetAndMetadata offset, ConsumerGroupMetadata groupMetadata) {
        producer.sendOffsetsToTransaction(Map.of(partition, offset), groupMetadata);
    }

    @Override
    public void commit() {
        producer.commitTransaction();
    }

    @Override
    public void abort() {
        producer.abortTransaction();
    }

    @Override
    public void close() {
        producer.close();
    }
}
