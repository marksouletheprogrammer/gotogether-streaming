package com.improving.gotogether.cuts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdempotentCutConsumerTest {
    @Test
    void upsertsByEventIdBeforeAdvancingTheSourceOffset() {
        Map<String, GenericRecord> rows = new HashMap<>();
        List<String> calls = new ArrayList<>();
        IdempotentCutConsumer consumer = new IdempotentCutConsumer(record -> {
            calls.add("upsert");
            rows.put(record.get("event_id").toString(), record);
        });
        GenericRecord first = new CutRecordGenerator().generate(1);
        GenericRecord replacement = new CutRecordGenerator().generate(2);
        replacement.put("event_id", first.get("event_id"));

        consumer.process(first, () -> calls.add("offset-1"));
        consumer.process(replacement, () -> calls.add("offset-2"));

        assertEquals(List.of("upsert", "offset-1", "upsert", "offset-2"), calls);
        assertEquals(1, rows.size());
        assertEquals(2, ((GenericRecord) rows.get(first.get("event_id").toString()).get("payload")).get("cut_index"));
    }

    @Test
    void databaseFailureDoesNotAdvanceTheSourceOffset() {
        IdempotentCutConsumer consumer = new IdempotentCutConsumer(record -> {
            throw new IllegalStateException("database unavailable");
        });
        int[] committedOffsets = {0};

        assertThrows(
            IllegalStateException.class,
            () -> consumer.process(new CutRecordGenerator().generate(1), () -> committedOffsets[0]++)
        );
        assertEquals(0, committedOffsets[0]);
    }

    @Test
    void invalidRecordsDoNotReachTheRepositoryOrAdvanceOffsets() {
        int[] writes = {0};
        IdempotentCutConsumer consumer = new IdempotentCutConsumer(record -> writes[0]++);
        GenericRecord invalid = new CutRecordGenerator().generate(1);
        invalid.put("machine_id", "other-mill");

        assertThrows(IllegalArgumentException.class, () -> consumer.process(invalid, () -> writes[0]++));
        assertEquals(0, writes[0]);
    }

    @Test
    void replayAfterDatabaseCommitBeforeOffsetCommitUpsertsByEventId() {
        // Scenario: Replay after database commit before Kafka offset commit
        // When replayed, the same event_id should not create a second row
        Map<String, GenericRecord> rows = new HashMap<>();
        List<String> calls = new ArrayList<>();
        IdempotentCutConsumer consumer = new IdempotentCutConsumer(record -> {
            calls.add("upsert");
            rows.put(record.get("event_id").toString(), record);
        });

        GenericRecord firstRecord = new CutRecordGenerator().generate(1);
        consumer.process(firstRecord, () -> calls.add("offset-1"));
        
        // Simulate replay: same event_id, should upsert not insert
        consumer.process(firstRecord, () -> calls.add("offset-2"));

        // Both writes should be for the same event_id (upsert behavior)
        assertEquals(1, rows.size());
        assertEquals(List.of("upsert", "offset-1", "upsert", "offset-2"), calls);
    }

    @Test
    void upsertChangedCutWithSameEventId() {
        // Scenario: Upsert a changed cut with the same ID
        // When a later cut with the same event_id arrives, it should replace the previous one
        Map<String, GenericRecord> rows = new HashMap<>();
        List<String> calls = new ArrayList<>();
        IdempotentCutConsumer consumer = new IdempotentCutConsumer(record -> {
            calls.add("upsert");
            rows.put(record.get("event_id").toString(), record);
        });

        GenericRecord firstRecord = new CutRecordGenerator().generate(1);
        GenericRecord secondRecord = new CutRecordGenerator().generate(2);
        // Force same event_id to simulate a changed cut
        secondRecord.put("event_id", firstRecord.get("event_id"));

        consumer.process(firstRecord, () -> calls.add("offset-1"));
        consumer.process(secondRecord, () -> calls.add("offset-2"));

        // Only one row should exist (upsert), with the second record's values
        assertEquals(1, rows.size());
        GenericRecord stored = rows.get(firstRecord.get("event_id").toString());
        assertEquals(2, ((GenericRecord) stored.get("payload")).get("cut_index"));
        assertEquals(List.of("upsert", "offset-1", "upsert", "offset-2"), calls);
    }
}
