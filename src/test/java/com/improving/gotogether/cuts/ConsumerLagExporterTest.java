package com.improving.gotogether.cuts;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumerLagExporterTest {
    @Test
    void reportsBacklogAndZeroLagForCaughtUpConsumerGroups() {
        assertEquals(5, ConsumerLagExporter.calculateLag(10L, 15L));
        assertEquals(0, ConsumerLagExporter.calculateLag(15L, 15L));
        assertEquals(0, ConsumerLagExporter.calculateLag(16L, 15L));
        assertEquals(15, ConsumerLagExporter.calculateLag(null, 15L));
    }
}
