package com.improving.gotogether.cuts;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public interface KafkaTransaction {
    void begin();

    void send(String topic, GenericRecord record);

    void sendOffsets(TopicPartition partition, OffsetAndMetadata offset, ConsumerGroupMetadata groupMetadata);

    void commit();

    void abort();
}
