package com.improving.gotogether.cuts;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventSchemasTest {
    @Test
    void derivesAnAvroEventSchemaWithNullableOptionalPayloadFields() {
        Schema schema = EventSchemas.eventSchema();
        Schema payload = schema.getField("payload").schema();

        assertEquals(Schema.Type.STRING, schema.getField("kind").schema().getType());
        assertEquals("event_type", payload.getField("event_type").name());
        assertEquals(Schema.Type.UNION, payload.getField("cut_index").schema().getType());
        assertEquals(Schema.Type.UNION, payload.getField("replacement_tool_instance_id").schema().getType());
    }

    @Test
    void validatesAndRoundTripsCanonicalInstallationAndInspectionEvents() {
        GenericRecord installed = event("tool_installed");
        EventSchemas.validateCanonicalEvent(installed);
        GenericRecord installedRoundTrip = EventSchemas.decode(EventSchemas.encode(installed), EventSchemas.eventSchema());
        assertEquals(installed, installedRoundTrip);
        assertEquals(EventSchemas.toMap(installed), EventSchemas.toMap(installedRoundTrip));

        GenericRecord inspection = event("inspection");
        GenericRecord payload = (GenericRecord) inspection.get("payload");
        payload.put("component", "cutter");
        payload.put("confirmed_fault", "none");
        EventSchemas.validateCanonicalEvent(inspection);
        assertEquals(inspection, EventSchemas.decode(EventSchemas.encode(inspection), EventSchemas.eventSchema()));
    }

    @Test
    void rejectsInvalidIdsKindsTimestampsAndConditionalPayloads() {
        GenericRecord emptyId = event("tool_installed");
        emptyId.put("event_id", " ");
        GenericRecord wrongKind = event("tool_installed");
        wrongKind.put("kind", "cut");
        GenericRecord malformedTimestamp = event("tool_installed");
        malformedTimestamp.put("occurred_at", "not-a-timestamp");
        GenericRecord incompleteInspection = event("inspection");
        GenericRecord badEnum = event("tool_installed");
        ((GenericRecord) badEnum.get("payload")).put("event_type", "unknown");

        assertThrows(IllegalArgumentException.class, () -> EventSchemas.validateCanonicalEvent(emptyId));
        assertThrows(IllegalArgumentException.class, () -> EventSchemas.validateCanonicalEvent(wrongKind));
        assertThrows(IllegalArgumentException.class, () -> EventSchemas.validateCanonicalEvent(malformedTimestamp));
        assertThrows(IllegalArgumentException.class, () -> EventSchemas.validateCanonicalEvent(incompleteInspection));
        assertThrows(IllegalArgumentException.class, () -> EventSchemas.validateCanonicalEvent(badEnum));
    }

    private static GenericRecord event(String eventType) {
        Schema schema = EventSchemas.eventSchema();
        GenericRecord record = new GenericData.Record(schema);
        record.put("schema_version", "demo-1");
        record.put("event_id", "event-cutter-1-001");
        record.put("kind", "event");
        record.put("occurred_at", "2026-03-04T10:15:00Z");
        record.put("machine_id", "mill-1");
        record.put("tool_instance_id", "cutter-1");

        Schema payloadSchema = schema.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("event_type", eventType);
        record.put("payload", payload);
        return record;
    }
}
