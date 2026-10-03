package com.improving.gotogether.cuts;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionalCutConsumerTest {
    private static final String OUTPUT_TOPIC = "mill-cuts-committed";
    private static final TopicPartition SOURCE_PARTITION = new TopicPartition("mill-cuts-transactional-source", 0);
    private static final ConsumerGroupMetadata GROUP = new ConsumerGroupMetadata("mill-cuts-transactional-consumer");

    @Test
    void writesDatabaseBeforeCommittingKafkaOutputAndOffset() {
        List<String> calls = new ArrayList<>();
        TransactionalCutConsumer consumer = new TransactionalCutConsumer(
            OUTPUT_TOPIC,
            new RecordingKafkaTransaction(calls),
            record -> calls.add("database")
        );

        consumer.process(new CutRecordGenerator().generate(1), SOURCE_PARTITION, new OffsetAndMetadata(1), GROUP);

        assertEquals(List.of("begin", "send", "database", "offsets", "commit"), calls);
    }

    @Test
    void abortsKafkaOutputAndOffsetWhenDatabaseWriteFails() {
        List<String> calls = new ArrayList<>();
        TransactionalCutConsumer consumer = new TransactionalCutConsumer(
            OUTPUT_TOPIC,
            new RecordingKafkaTransaction(calls),
            record -> {
                calls.add("database");
                throw new IllegalStateException("database unavailable");
            }
        );

        assertThrows(
            IllegalStateException.class,
            () -> consumer.process(new CutRecordGenerator().generate(1), SOURCE_PARTITION, new OffsetAndMetadata(1), GROUP)
        );
        assertEquals(List.of("begin", "send", "database", "abort"), calls);
    }

    @Test
    void abortsKafkaOutputWhenTransactionalOffsetCommitFails() {
        List<String> calls = new ArrayList<>();
        TransactionalCutConsumer consumer = new TransactionalCutConsumer(
            OUTPUT_TOPIC,
            new RecordingKafkaTransaction(calls, true),
            record -> calls.add("database")
        );

        assertThrows(
            IllegalStateException.class,
            () -> consumer.process(new CutRecordGenerator().generate(1), SOURCE_PARTITION, new OffsetAndMetadata(1), GROUP)
        );
        assertEquals(List.of("begin", "send", "database", "offsets", "abort"), calls);
    }

    private static final class RecordingKafkaTransaction implements KafkaTransaction {
        private final List<String> calls;
        private final boolean failOnOffsets;

        private RecordingKafkaTransaction(List<String> calls) {
            this(calls, false);
        }

        private RecordingKafkaTransaction(List<String> calls, boolean failOnOffsets) {
            this.calls = calls;
            this.failOnOffsets = failOnOffsets;
        }

        @Override
        public void begin() {
            calls.add("begin");
        }

        @Override
        public void send(String topic, GenericRecord record) {
            calls.add("send");
        }

        @Override
        public void sendOffsets(
            TopicPartition partition,
            OffsetAndMetadata offset,
            ConsumerGroupMetadata groupMetadata
        ) {
            calls.add("offsets");
            if (failOnOffsets) {
                throw new IllegalStateException("group generation changed");
            }
        }

        @Override
        public void commit() {
            calls.add("commit");
        }

        @Override
        public void abort() {
            calls.add("abort");
        }
    }
}
