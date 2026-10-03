package com.improving.gotogether.cuts;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventRecordGeneratorTest {
    @Test
    void generatesInstallationsBeforeRepeatedInspectionsForMultipleTools() {
        EventRecordGenerator generator = new EventRecordGenerator();
        List<GenericRecord> records = IntStream.rangeClosed(1, 9).mapToObj(generator::generate).toList();

        assertEquals(9, records.size());
        assertEquals(Set.of("cutter-1", "cutter-2", "cutter-3"), records.stream()
            .map(record -> record.get("tool_instance_id").toString())
            .collect(Collectors.toSet()));
        for (int index = 0; index < 3; index++) {
            assertEquals("tool_installed", payload(records.get(index)).get("event_type").toString());
        }
        for (int index = 3; index < records.size(); index++) {
            assertEquals("inspection", payload(records.get(index)).get("event_type").toString());
        }
        assertEquals("cutter-1", records.get(0).get("tool_instance_id").toString());
        assertEquals("cutter-1", records.get(3).get("tool_instance_id").toString());
        records.forEach(EventSchemas::validateCanonicalEvent);
    }

    @Test
    void generatedIdsAreUniqueAndTheSeriesIsStableAcrossRestarts() {
        EventRecordGenerator first = new EventRecordGenerator();
        EventRecordGenerator restarted = new EventRecordGenerator();
        List<GenericRecord> series = new ArrayList<>();
        for (int index = 1; index <= 9; index++) {
            GenericRecord event = first.generate(index);
            assertNotEquals("", event.get("event_id").toString());
            assertEquals(event, restarted.generate(index));
            series.add(event);
        }
        assertEquals(9, series.stream().map(record -> record.get("event_id").toString()).distinct().count());
    }

    @Test
    void rejectsNonpositiveEventIndex() {
        assertThrows(IllegalArgumentException.class, () -> new EventRecordGenerator().generate(0));
    }

    private static GenericRecord payload(GenericRecord event) {
        return (GenericRecord) event.get("payload");
    }
}
