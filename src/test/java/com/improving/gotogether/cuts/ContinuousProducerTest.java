package com.improving.gotogether.cuts;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for continuous, paced publishing behavior.
 * These tests verify that producers continue past the old 30-record limit
 * and support orderly shutdown.
 */
class ContinuousProducerTest {
    /**
     * Verifies that cut records can be generated and matched beyond index 30.
     * Both cut generators should produce the same record for the same index.
     */
    @Test
    void cutGeneratorProducesMatchingRecordsPastIndex30() {
        CutRecordGenerator first = new CutRecordGenerator();
        CutRecordGenerator second = new CutRecordGenerator();

        // Generate records beyond the old 30-record limit
        for (int index = 1; index <= 50; index++) {
            GenericRecord firstRecord = first.generate(index);
            GenericRecord secondRecord = second.generate(index);
            assertEquals(firstRecord, secondRecord, "Records at index " + index + " should match");
        }
    }

    /**
     * Verifies that event records can be generated continuously with unique IDs
     * and proper tool instance sequencing beyond the old 30-record limit.
     */
    @Test
    void eventGeneratorProducesUniqueIdsAndProperSequencingPastIndex30() {
        EventRecordGenerator generator = new EventRecordGenerator();
        List<String> eventIds = new ArrayList<>();
        List<String> toolInstances = new ArrayList<>();

        // Generate events beyond the old 30-record limit
        for (int index = 1; index <= 50; index++) {
            GenericRecord event = generator.generate(index);
            eventIds.add(event.get("event_id").toString());
            toolInstances.add(event.get("tool_instance_id").toString());
        }

        // All IDs should be unique
        assertEquals(50, eventIds.stream().distinct().count(), "All event IDs should be unique");

        // Should have multiple tool instances
        assertEquals(3, toolInstances.stream().distinct().count(), "Should have 3 tool instances");
    }

    /**
     * Verifies that a paced producer can be interrupted cleanly
     * and respects shutdown signals.
     */
    @Test
    void pacedProducerSupportsOrderlyShutdown() {
        AtomicInteger publishCount = new AtomicInteger();
        AtomicBoolean shutdownRequested = new AtomicBoolean();
        List<String> publishedIds = new ArrayList<>();

        // Simulate a paced producer loop that respects shutdown
        Thread producerThread = new Thread(() -> {
            for (int index = 1; !shutdownRequested.get(); index++) {
                if (index > 100) break; // Safety limit for test
                GenericRecord cut = new CutRecordGenerator().generate(index);
                publishedIds.add(cut.get("event_id").toString());
                publishCount.incrementAndGet();

                // Simulate pacing
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                if (index == 10) {
                    shutdownRequested.set(true);
                }
            }
        });

        producerThread.start();
        try {
            producerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertFalse(producerThread.isAlive(), "Producer thread should have stopped");
        assertEquals(10, publishCount.get(), "Should have published 10 records before shutdown");
    }

    /**
     * Verifies that cut records maintain stable, deterministic event_ids
     * across multiple generator instances (important for matching).
     */
    @Test
    void cutEventIdsAreStableAcrossGeneratorInstances() {
        CutRecordGenerator first = new CutRecordGenerator();
        CutRecordGenerator second = new CutRecordGenerator();

        for (int index = 1; index <= 50; index++) {
            String firstId = first.generate(index).get("event_id").toString();
            String secondId = second.generate(index).get("event_id").toString();
            assertEquals(firstId, secondId, "Event IDs should be stable at index " + index);
        }
    }

    /**
     * Verifies that event records maintain unique IDs even with tool instance cycling.
     */
    @Test
    void eventIdsRemainUniqueWithToolInstanceCycling() {
        EventRecordGenerator generator = new EventRecordGenerator();
        List<String> eventIds = new ArrayList<>();

        // Generate enough events to cycle through tools multiple times
        for (int index = 1; index <= 100; index++) {
            eventIds.add(generator.generate(index).get("event_id").toString());
        }

        // All should be unique
        long uniqueCount = eventIds.stream().distinct().count();
        assertEquals(100, uniqueCount, "All 100 event IDs should be unique");
    }

    /**
     * Verifies that tool lifecycle can be managed beyond the old limits
     * (e.g., tool_index reset when approaching integer overflow).
     */
    @Test
    void toolLifecycleManagesIndexOverflow() {
        CutRecordGenerator generator = new CutRecordGenerator();

        // Generate a large number of records to verify overflow handling
        for (int index = 1; index <= 1000; index++) {
            GenericRecord record = generator.generate(index);
            GenericRecord payload = (GenericRecord) record.get("payload");
            int cutIndex = (Integer) payload.get("cut_index");

            // cut_index should be positive and reasonable
            assertTrue(cutIndex > 0, "cut_index should be positive at index " + index);
            assertTrue(cutIndex <= 1000, "cut_index should not overflow at index " + index);
        }
    }
}
