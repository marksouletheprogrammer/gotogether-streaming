package com.improving.gotogether.cuts;

import java.time.Instant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class EventRecordGenerator {
    private static final Instant START_TIME = Instant.parse("2026-03-04T10:15:00Z");
    private static final int TOOL_COUNT = 3;

    public GenericRecord generate(int eventIndex) {
        if (eventIndex < 1) {
            throw new IllegalArgumentException("eventIndex must be positive");
        }

        int toolNumber = eventIndex <= TOOL_COUNT ? eventIndex : (eventIndex - TOOL_COUNT - 1) % TOOL_COUNT + 1;
        String toolInstanceId = "cutter-" + toolNumber;
        String eventType = eventIndex <= TOOL_COUNT ? "tool_installed" : "inspection";
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
