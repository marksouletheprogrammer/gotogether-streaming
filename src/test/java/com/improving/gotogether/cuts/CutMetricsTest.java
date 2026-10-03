package com.improving.gotogether.cuts;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CutMetricsTest {
    @Test
    void tracksProducerSendsAndFailuresWithItsClientIdentity() {
        try (CutMetrics metrics = CutMetrics.producer("mill-cuts-test-producer")) {
            metrics.recordSend();
            metrics.recordSendFailure();

            assertEquals("producer", metrics.role());
            assertEquals("mill-cuts-test-producer", metrics.identity());
            assertEquals(1, metrics.getRecordsSentTotal());
            assertEquals(1, metrics.getSendErrorsTotal());
        }
    }

    @Test
    void tracksConsumerThroughputAndFailuresWithItsGroupIdentity() {
        try (CutMetrics metrics = CutMetrics.consumer("mill-cuts-test-consumer")) {
            metrics.recordProcessed();
            metrics.recordProcessingFailure();

            assertEquals("consumer", metrics.role());
            assertEquals("mill-cuts-test-consumer", metrics.identity());
            assertEquals(1, metrics.getRecordsProcessedTotal());
            assertEquals(1, metrics.getProcessingErrorsTotal());
        }
    }
}
