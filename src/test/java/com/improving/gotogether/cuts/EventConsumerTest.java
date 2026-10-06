package com.improving.gotogether.cuts;

import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.fail;

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

    @Test
    void rejectsInvalidDlqProbability() {
        RecordingRepository repository = new RecordingRepository();
        assertThrows(IllegalArgumentException.class, () -> new EventConsumer(repository, (event, error) -> { }, -0.1, () -> 0));
        assertThrows(IllegalArgumentException.class, () -> new EventConsumer(repository, (event, error) -> { }, 1.1, () -> 0));
        assertThrows(IllegalArgumentException.class, () -> new EventConsumer(repository, (event, error) -> { }, Double.NaN, () -> 0));
    }

    @Test
    void supportsMutuallyExclusiveDlqAndSilentDropOutcomes() {
        // This test verifies that the consumer supports three-way selection:
        // - DLQ failure (when random < dlqProbability)
        // - Silent drop (when dlqProbability <= random < dlqProbability + silentProbability)
        // - Success (when random >= dlqProbability + silentProbability)
        RecordingRepository repository = new RecordingRepository();
        List<String> dlqPublishes = new ArrayList<>();
        EventConsumer consumer = new EventConsumer(
            repository,
            (event, error) -> dlqPublishes.add("dlq"),
            0.3,  // 30% DLQ
            0.2,  // 20% silent drop
            () -> 0.35  // Falls in silent drop range [0.3, 0.5)
        );

        assertEquals(EventConsumer.Outcome.SILENTLY_DROPPED, consumer.process(new EventRecordGenerator().generate(1), () -> { }));
        assertEquals(0, dlqPublishes.size());
        assertFalse(repository.isProcessed("event-cutter-1-001"));
    }

    @Test
    void silentDropLeavesReconciliationUnprocessedWithoutDlqOrTargetWrite() {
        // Scenario: Simulate an unreported dropped event
        // When a silent drop is selected, the source offset advances but:
        // - reconciliation row remains unprocessed
        // - no target row changes
        // - no DLQ record is published
        RecordingRepository repository = new RecordingRepository();
        List<String> dlqPublishes = new ArrayList<>();
        EventConsumer consumer = new EventConsumer(
            repository,
            (event, error) -> dlqPublishes.add("dlq"),
            0.0,  // 0% DLQ
            1.0,  // 100% silent drop
            () -> 0.5
        );
        GenericRecord event = new EventRecordGenerator().generate(1);

        assertEquals(EventConsumer.Outcome.SILENTLY_DROPPED, consumer.process(event, () -> { }));

        assertFalse(repository.isProcessed(event.get("event_id").toString()));
        assertEquals(0, repository.targetUpdates);
        assertEquals(0, dlqPublishes.size());
    }

    @Test
    void silentDropAndDlqAreIndependent() {
        // Scenario: Observe silent loss separately from DLQ count
        // When one event is silently dropped and another is dead-lettered,
        // both should have unprocessed reconciliation rows, but only the
        // dead-lettered event should appear in the DLQ topic
        RecordingRepository repository = new RecordingRepository();
        List<String> dlqPublishes = new ArrayList<>();
        
        // First consumer: DLQ
        EventConsumer dlqConsumer = new EventConsumer(
            repository,
            (event, error) -> dlqPublishes.add("dlq"),
            1.0,  // 100% DLQ
            0.0,  // 0% silent drop
            () -> 0.0
        );
        
        // Second consumer: silent drop
        EventConsumer silentConsumer = new EventConsumer(
            repository,
            (event, error) -> dlqPublishes.add("dlq"),
            0.0,  // 0% DLQ
            1.0,  // 100% silent drop
            () -> 0.5
        );

        GenericRecord event1 = new EventRecordGenerator().generate(1);
        GenericRecord event2 = new EventRecordGenerator().generate(2);

        assertEquals(EventConsumer.Outcome.DEAD_LETTERED, dlqConsumer.process(event1, () -> { }));
        assertEquals(EventConsumer.Outcome.SILENTLY_DROPPED, silentConsumer.process(event2, () -> { }));

        // Both should be unprocessed
        assertFalse(repository.isProcessed(event1.get("event_id").toString()));
        assertFalse(repository.isProcessed(event2.get("event_id").toString()));
        // But only one should be in DLQ
        assertEquals(1, dlqPublishes.size());
    }

    @Test
    void disablingInjectedOutcomesProcessesNormally() {
        // Scenario: Disable injected outcomes
        // When both DLQ and silent drop probabilities are zero,
        // valid events should be processed normally
        RecordingRepository repository = new RecordingRepository();
        EventConsumer consumer = new EventConsumer(
            repository,
            (event, error) -> { },
            0.0,  // 0% DLQ
            0.0,  // 0% silent drop
            () -> 0.5
        );

        assertEquals(EventConsumer.Outcome.PROCESSED, consumer.process(new EventRecordGenerator().generate(1), () -> { }));
        assertTrue(repository.isProcessed("event-cutter-1-001"));
        assertEquals(1, repository.targetUpdates);
    }

    @Test
    void preservesRetryabilityForRealFailures() {
        // Scenario: Preserve retryability for real failures
        // When persistence fails or DLQ publication is not confirmed,
        // the source offset should not be committed
        EventStateRepository repository = new EventStateRepository() {
            @Override
            public boolean isProcessed(String eventId) {
                return false;
            }

            @Override
            public boolean applyIfUnprocessed(GenericRecord event) {
                throw new IllegalStateException("database unavailable");
            }
        };
        EventConsumer consumer = new EventConsumer(
            repository,
            (event, error) -> { },
            0.0,  // 0% DLQ
            0.0,  // 0% silent drop
            () -> 0.5
        );
        AtomicInteger offsets = new AtomicInteger();

        assertThrows(IllegalStateException.class,
            () -> consumer.process(new EventRecordGenerator().generate(1), offsets::incrementAndGet));
        assertEquals(0, offsets.get());
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
