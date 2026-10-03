package com.improving.gotogether.cuts;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public final class TransactionalCutConsumer {
    private final String processedTopic;
    private final KafkaTransaction kafka;
    private final CutRecordRepository repository;

    public TransactionalCutConsumer(String processedTopic, KafkaTransaction kafka, CutRecordRepository repository) {
        this.processedTopic = processedTopic;
        this.kafka = kafka;
        this.repository = repository;
    }

    public void process(
        GenericRecord record,
        TopicPartition sourcePartition,
        OffsetAndMetadata nextOffset,
        ConsumerGroupMetadata groupMetadata
    ) {
        CutSchemas.validateCanonicalCut(record);
        kafka.begin();
        try {
            kafka.send(processedTopic, record);
            repository.insert(record);
            kafka.sendOffsets(sourcePartition, nextOffset, groupMetadata);
            kafka.commit();
        } catch (RuntimeException failure) {
            try {
                kafka.abort();
            } catch (RuntimeException abortFailure) {
                failure.addSuppressed(abortFailure);
            }
            throw failure;
        }
    }
}
