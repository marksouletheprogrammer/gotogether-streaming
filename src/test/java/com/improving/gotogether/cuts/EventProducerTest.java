package com.improving.gotogether.cuts;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventProducerTest {
    @Test
    void commitsTheReconciliationRowBeforeSending() {
        RecordingRepository repository = new RecordingRepository();
        AtomicBoolean sent = new AtomicBoolean();
        EventProducer producer = new EventProducer(repository, event -> {
            assertTrue(repository.rows.containsKey(event.get("event_id").toString()));
            assertFalse(repository.rows.get(event.get("event_id").toString()));
            sent.set(true);
        });

        producer.publish(1);

        assertTrue(sent.get());
        assertEquals(1, repository.rows.size());
    }

    @Test
    void doesNotSendWhenTheReconciliationCommitFails() {
        EventReconciliationRepository repository = eventId -> {
            throw new IllegalStateException("database unavailable");
        };
        AtomicBoolean sent = new AtomicBoolean();
        EventProducer producer = new EventProducer(repository, event -> sent.set(true));

        assertThrows(IllegalStateException.class, () -> producer.publish(1));
        assertFalse(sent.get());
    }

    @Test
    void leavesTheCommittedRowUnprocessedWhenKafkaSendFails() {
        RecordingRepository repository = new RecordingRepository();
        EventProducer producer = new EventProducer(repository, event -> {
            throw new IllegalStateException("kafka unavailable");
        });

        assertThrows(IllegalStateException.class, () -> producer.publish(1));
        assertEquals(Map.of("event-cutter-1-001", false), repository.rows);
    }

    @Test
    void repeatedIdsDoNotDuplicateOrResetAProcessedRow() {
        RecordingRepository repository = new RecordingRepository();
        EventProducer producer = new EventProducer(repository, event -> { });

        producer.publish(1);
        repository.rows.put("event-cutter-1-001", true);
        producer.publish(1);

        assertEquals(Map.of("event-cutter-1-001", true), repository.rows);
    }

    private static final class RecordingRepository implements EventReconciliationRepository {
        private final Map<String, Boolean> rows = new HashMap<>();

        @Override
        public void ensureEventRow(String eventId) {
            rows.putIfAbsent(eventId, false);
        }
    }
}
