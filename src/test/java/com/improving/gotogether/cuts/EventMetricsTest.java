package com.improving.gotogether.cuts;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventMetricsTest {
    @Test
    void exposesEventProducerAndConsumerCountersWithDistinctIdentities() {
        try (EventMetrics producer = EventMetrics.producer("mill-tool-events-producer");
             EventMetrics consumer = EventMetrics.consumer("mill-tool-events-consumer")) {
            producer.recordSend();
            producer.recordSendFailure();
            consumer.recordProcessed();
            consumer.recordProcessingFailure();

            assertEquals("producer", producer.role());
            assertEquals("mill-tool-events-producer", producer.identity());
            assertEquals(1, producer.getRecordsSentTotal());
            assertEquals(1, producer.getSendErrorsTotal());
            assertEquals("consumer", consumer.role());
            assertEquals("mill-tool-events-consumer", consumer.identity());
            assertEquals(1, consumer.getRecordsProcessedTotal());
            assertEquals(1, consumer.getProcessingErrorsTotal());
        }
    }
}
