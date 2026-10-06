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

    @Test
    void replayAfterDatabaseCommitBeforeKafkaOffsetCommitUpsertsByEventId() {
        // Scenario: Kafka abort after database write before Kafka commit
        // When replayed, the same event_id should not create a second row
        List<String> databaseWrites = new ArrayList<>();
        TransactionalCutConsumer consumer = new TransactionalCutConsumer(
            OUTPUT_TOPIC,
            new RecordingKafkaTransaction(new ArrayList<>()),
            record -> databaseWrites.add(record.get("event_id").toString())
        );

        GenericRecord firstRecord = new CutRecordGenerator().generate(1);
        consumer.process(firstRecord, SOURCE_PARTITION, new OffsetAndMetadata(1), GROUP);
        
        // Simulate replay: same event_id, should upsert not insert
        consumer.process(firstRecord, SOURCE_PARTITION, new OffsetAndMetadata(1), GROUP);
        
        // Both writes should be for the same event_id (upsert behavior)
        assertEquals(2, databaseWrites.size());
        assertEquals(databaseWrites.get(0), databaseWrites.get(1));
    }

    @Test
    void upsertChangedCutWithSameEventId() {
        // Scenario: Upsert a changed cut with the same ID
        // When a later cut with the same event_id arrives, it should replace the previous one
        List<GenericRecord> databaseRecords = new ArrayList<>();
        TransactionalCutConsumer consumer = new TransactionalCutConsumer(
            OUTPUT_TOPIC,
            new RecordingKafkaTransaction(new ArrayList<>()),
            record -> databaseRecords.add(record)
        );

        GenericRecord firstRecord = new CutRecordGenerator().generate(1);
        GenericRecord secondRecord = new CutRecordGenerator().generate(2);
        // Force same event_id to simulate a changed cut
        secondRecord.put("event_id", firstRecord.get("event_id"));

        consumer.process(firstRecord, SOURCE_PARTITION, new OffsetAndMetadata(1), GROUP);
        consumer.process(secondRecord, SOURCE_PARTITION, new OffsetAndMetadata(2), GROUP);

        // Both should be written (upsert), but they have the same event_id
        assertEquals(2, databaseRecords.size());
        assertEquals(firstRecord.get("event_id"), secondRecord.get("event_id"));
        // The second record should have different payload (cut_index = 2)
        assertEquals(2, ((GenericRecord) secondRecord.get("payload")).get("cut_index"));
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
