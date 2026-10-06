package com.improving.gotogether.cuts;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerLagExporterTest {
    @Test
    void reportsBacklogAndZeroLagForCaughtUpConsumerGroups() {
        assertEquals(5, ConsumerLagExporter.calculateLag(10L, 15L));
        assertEquals(0, ConsumerLagExporter.calculateLag(15L, 15L));
        assertEquals(0, ConsumerLagExporter.calculateLag(16L, 15L));
        assertEquals(15, ConsumerLagExporter.calculateLag(null, 15L));
    }

    @Test
    void includesEventConsumerLagWithoutReplacingCutTargets() {
        assertTrue(ConsumerLagExporter.TARGETS.contains(
            new ConsumerLagExporter.LagTarget("mill-tool-events-consumer", "mill-tool-events-source")
        ));
        assertTrue(ConsumerLagExporter.TARGETS.contains(
            new ConsumerLagExporter.LagTarget("mill-cuts-idempotent-consumer", "mill-cuts-atleastonce-source")
        ));
    }

    @Test
    void sumsRetainedDlqRecordsAcrossPartitionsIncludingDuplicateCopies() {
        TopicPartition first = new TopicPartition("mill-tool-events-dlq", 0);
        TopicPartition second = new TopicPartition("mill-tool-events-dlq", 1);

        assertEquals(7, ConsumerLagExporter.calculateTopicLength(
            Map.of(first, 10L, second, 4L),
            Map.of(first, 15L, second, 6L)
        ));
        assertEquals(0, ConsumerLagExporter.calculateTopicLength(Map.of(first, 10L), Map.of(first, 10L)));
    }

    @Test
    void dlqCollectionFailureDoesNotHideHealthyLagOrReportFalseZero() {
        String metrics = ConsumerLagExporter.renderMetrics(
            List.of(new ConsumerLagExporter.LagSample("mill-cuts-idempotent-consumer", "mill-cuts-atleastonce-source", 0, 5)),
            true,
            null,
            false
        );

        assertTrue(metrics.contains("mill_cuts_consumer_group_lag{group_id=\"mill-cuts-idempotent-consumer\""));
        assertTrue(metrics.contains("mill_cuts_consumer_lag_collection_success 1"));
        assertTrue(metrics.contains("mill_tool_events_dlq_collection_success 0"));
        assertFalse(metrics.contains("mill_tool_events_dlq_topic_length "));
    }
}
