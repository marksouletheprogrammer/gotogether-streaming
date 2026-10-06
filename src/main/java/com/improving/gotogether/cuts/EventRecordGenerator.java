package com.improving.gotogether.cuts;

import java.time.Instant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class EventRecordGenerator {
    private static final Instant START_TIME = Instant.parse("2026-03-04T10:15:00Z");
    private static final int TOOL_COUNT = 3;

    public GenericRecord generate(long eventIndex) {
        if (eventIndex < 1) {
            throw new IllegalArgumentException("eventIndex must be positive");
        }

        // Determine tool number and event type
        // First TOOL_COUNT events are installations (one per tool)
        // Subsequent events cycle through tools as inspections
        int toolNumber;
        String eventType;
        if (eventIndex <= TOOL_COUNT) {
            toolNumber = (int) eventIndex;
            eventType = "tool_installed";
        } else {
            toolNumber = (int) ((eventIndex - TOOL_COUNT - 1) % TOOL_COUNT) + 1;
            eventType = "inspection";
        }

        String toolInstanceId = "cutter-" + toolNumber;
        Schema schema = EventSchemas.eventSchema();
        GenericRecord event = new GenericData.Record(schema);
        event.put("schema_version", "demo-1");
        event.put("event_id", "event-" + toolInstanceId + "-" + String.format("%03d", eventIndex));
        event.put("kind", "event");
        event.put("occurred_at", START_TIME.plusSeconds(2L * (eventIndex - 1)).toString());
        event.put("machine_id", "mill-1");
        event.put("tool_instance_id", toolInstanceId);

        Schema payloadSchema = schema.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("event_type", eventType);
        payload.put("cut_index", null);
        payload.put("component", "inspection".equals(eventType) ? "cutter" : null);
        payload.put("confirmed_fault", "inspection".equals(eventType)
            ? (eventIndex % 2 == 0 ? "none" : "unknown")
            : null);
        payload.put("planned", null);
        payload.put("replacement_tool_instance_id", null);
        event.put("payload", payload);

        EventSchemas.validateCanonicalEvent(event);
        return event;
    }
}
