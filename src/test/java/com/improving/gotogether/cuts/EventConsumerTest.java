package com.improving.gotogether.cuts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventConsumerTest {
    @Test
    void commitsOffsetOnlyAfterTargetAndProcessedStateAreCommitted() {
        RecordingRepository repository = new RecordingRepository();
        EventConsumer consumer = new EventConsumer(repository, (event, error) -> { }, 0, () -> 1);
        AtomicInteger offsets = new AtomicInteger();

        assertEquals(EventConsumer.Outcome.PROCESSED, consumer.process(new EventRecordGenerator().generate(1), () -> {
            assertTrue(repository.isProcessed("event-cutter-1-001"));
            assertEquals(1, repository.targetUpdates);
            offsets.incrementAndGet();
        }));

        assertEquals(1, offsets.get());
    }

    @Test
    void confirmsDlqPublishBeforeCommittingTheSourceOffset() {
        RecordingRepository repository = new RecordingRepository();
        List<String> operations = new ArrayList<>();
        EventConsumer consumer = new EventConsumer(repository, (event, error) -> operations.add("dlq-ack"), 1, () -> 0);

        consumer.process(new EventRecordGenerator().generate(1), () -> operations.add("offset-commit"));

        assertEquals(List.of("dlq-ack", "offset-commit"), operations);
    }

    @Test
    void leavesOffsetUncommittedWhenDlqPublishFails() {
        RecordingRepository repository = new RecordingRepository();
        EventConsumer consumer = new EventConsumer(repository, (event, error) -> {
            throw new IllegalStateException("dlq unavailable");
        }, 1, () -> 0);
        AtomicInteger offsets = new AtomicInteger();

        assertThrows(IllegalStateException.class,
            () -> consumer.process(new EventRecordGenerator().generate(1), offsets::incrementAndGet));
        assertEquals(0, offsets.get());
        assertFalse(repository.isProcessed("event-cutter-1-001"));
    }

    @Test
    void replayOfProcessedEventDoesNotUpdateTargetAgain() {
        RecordingRepository repository = new RecordingRepository();
        EventConsumer consumer = new EventConsumer(repository, (event, error) -> { }, 0, () -> 0);
        GenericRecord event = new EventRecordGenerator().generate(1);
        consumer.process(event, () -> { });
        int previousUpdates = repository.targetUpdates;

        assertEquals(EventConsumer.Outcome.ALREADY_PROCESSED, consumer.process(event, () -> { }));

        assertEquals(previousUpdates, repository.targetUpdates);
    }

    @Test
    void leavesOffsetUncommittedWhenDatabaseProcessingFails() {
        EventStateRepository repository = new RecordingRepository() {
            @Override
            public boolean applyIfUnprocessed(GenericRecord event) {
                throw new IllegalStateException("database unavailable");
            }
        };
        EventConsumer consumer = new EventConsumer(repository, (event, error) -> { }, 0, () -> 1);
        AtomicInteger offsets = new AtomicInteger();

        assertThrows(IllegalStateException.class,
            () -> consumer.process(new EventRecordGenerator().generate(1), offsets::incrementAndGet));
        assertEquals(0, offsets.get());
    }

    @Test
    void simulatedFailureLeavesTheReconciliationRowUnprocessedAndDoesNotUpdateTarget() {
        RecordingRepository repository = new RecordingRepository();
        EventConsumer consumer = new EventConsumer(repository, (event, error) -> { }, 1, () -> 0);
        GenericRecord event = new EventRecordGenerator().generate(1);

        assertEquals(EventConsumer.Outcome.DEAD_LETTERED, consumer.process(event, () -> { }));

        assertFalse(repository.isProcessed(event.get("event_id").toString()));
        assertEquals(0, repository.targetUpdates);
    }

    private static class RecordingRepository implements EventStateRepository {
        private final Map<String, Boolean> processed = new HashMap<>();
        private int targetUpdates;

        @Override
        public boolean isProcessed(String eventId) {
            return processed.getOrDefault(eventId, false);
        }

        @Override
        public boolean applyIfUnprocessed(GenericRecord event) {
            String eventId = event.get("event_id").toString();
            if (isProcessed(eventId)) {
                return false;
            }
            targetUpdates++;
            processed.put(eventId, true);
            return true;
        }
    }
}
