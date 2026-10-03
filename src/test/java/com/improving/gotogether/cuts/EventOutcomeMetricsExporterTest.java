package com.improving.gotogether.cuts;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventOutcomeMetricsExporterTest {
    @Test
    void computesAverageAgeAcrossUniqueEntityRows() {
        Instant now = Instant.parse("2026-03-04T10:15:10Z");

        assertEquals(5.0, EventOutcomeMetricsExporter.averageStalenessSeconds(List.of(
            Instant.parse("2026-03-04T10:15:00Z"),
            Instant.parse("2026-03-04T10:15:10Z")
        ), now));
    }

    @Test
    void emptyTargetTableOmitsStalenessAndHealthyEmptyCountsRemainZero() {
        String metrics = EventOutcomeMetricsExporter.renderMetrics(0, null, true);

        assertTrue(metrics.contains("mill_tool_events_unreconciled 0"));
        assertTrue(metrics.contains("mill_tool_events_outcome_collection_success 1"));
        assertFalse(metrics.contains("mill_tool_events_average_staleness_seconds "));
    }

    @Test
    void failedDatabaseCollectionDoesNotExposeStaleOutcomeValues() {
        String metrics = EventOutcomeMetricsExporter.renderMetrics(7, 12.0, false);

        assertEquals("mill_tool_events_outcome_collection_success 0\n", metrics);
        assertFalse(metrics.contains("mill_tool_events_unreconciled "));
        assertFalse(metrics.contains("mill_tool_events_average_staleness_seconds "));
    }
}
