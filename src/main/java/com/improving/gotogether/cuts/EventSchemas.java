package com.improving.gotogether.cuts;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public final class EventSchemas {
    private static final String SCHEMA_RESOURCE = "/avro/event-record.avsc";
    private static final Set<String> EVENT_TYPES = Set.of("tool_installed", "inspection", "maintenance");
    private static final Set<String> COMPONENTS = Set.of("cutter", "coolant_system");
    private static final Set<String> FAULTS = Set.of("none", "chipped_cutter", "low_coolant_flow", "unknown");
    private static final Schema EVENT_SCHEMA = loadSchema();

    private EventSchemas() {
    }

    public static Schema eventSchema() {
        return EVENT_SCHEMA;
    }

    public static byte[] encode(GenericRecord record) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericRecord>(EVENT_SCHEMA).write(record, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not encode event Avro record", exception);
        }
    }

    public static GenericRecord decode(byte[] encoded, Schema schema) {
        try {
            var decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(encoded), null);
            return new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
        } catch (IOException exception) {
            throw new IllegalArgumentException("Could not decode event Avro record", exception);
        }
    }

    public static Map<String, Object> toMap(GenericRecord record) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            Object canonicalValue = value instanceof GenericRecord nested
                ? toMap(nested)
                : value instanceof CharSequence text ? text.toString() : value;
            values.put(field.name(), canonicalValue);
        }
        return values;
    }

    public static void validateCanonicalEvent(GenericRecord record) {
        if (!EVENT_SCHEMA.equals(record.getSchema())) {
            throw new IllegalArgumentException("Event record does not use the canonical Avro schema");
        }
        requireString(record.get("schema_version"), "demo-1", "schema_version");
        requireNonblank(record.get("event_id"), "event_id");
        requireString(record.get("kind"), "event", "kind");
        requireTimestamp(record.get("occurred_at"));
        requireString(record.get("machine_id"), "mill-1", "machine_id");
        requireNonblank(record.get("tool_instance_id"), "tool_instance_id");

        Object payloadValue = record.get("payload");
        if (!(payloadValue instanceof GenericRecord payload)) {
            throw new IllegalArgumentException("Event payload must be an Avro record");
        }
        String eventType = value(payload.get("event_type"));
        if (!EVENT_TYPES.contains(eventType)) {
            throw new IllegalArgumentException("event_type is not a canonical event type");
        }
        validateOptionalCutIndex(payload.get("cut_index"));
        validateOptionalEnum(payload.get("component"), COMPONENTS, "component");
        validateOptionalEnum(payload.get("confirmed_fault"), FAULTS, "confirmed_fault");
        validateOptionalBoolean(payload.get("planned"), "planned");
        validateOptionalId(payload.get("replacement_tool_instance_id"), "replacement_tool_instance_id");

        if ("inspection".equals(eventType)) {
            requirePresent(payload.get("component"), "component");
            requirePresent(payload.get("confirmed_fault"), "confirmed_fault");
        } else if ("maintenance".equals(eventType)) {
            requirePresent(payload.get("component"), "component");
            requirePresent(payload.get("confirmed_fault"), "confirmed_fault");
            requirePresent(payload.get("planned"), "planned");
        }
    }

    private static Schema loadSchema() {
        try (InputStream input = EventSchemas.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("Missing canonical event Avro schema: " + SCHEMA_RESOURCE);
            }
            return new Schema.Parser().parse(input);
        } catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static String value(Object value) {
        return value == null ? null : value.toString();
    }

    private static void requireString(Object value, String expected, String field) {
        if (!expected.equals(value(value))) {
            throw new IllegalArgumentException(field + " must be " + expected);
        }
    }

    private static void requireNonblank(Object value, String field) {
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException(field + " must be non-empty");
        }
    }

    private static void requireTimestamp(Object value) {
        try {
            OffsetDateTime.parse(value == null ? "" : value.toString());
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("occurred_at must be an RFC 3339 timestamp", exception);
        }
    }

    private static void requirePresent(Object value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " is required for this event_type");
        }
    }

    private static void validateOptionalEnum(Object value, Set<String> allowed, String field) {
        if (value != null && !allowed.contains(value.toString())) {
            throw new IllegalArgumentException(field + " is not a canonical value");
        }
    }

    private static void validateOptionalCutIndex(Object value) {
        if (value != null && (!(value instanceof Integer index) || index < 1)) {
            throw new IllegalArgumentException("cut_index must be a positive integer");
        }
    }

    private static void validateOptionalBoolean(Object value, String field) {
        if (value != null && !(value instanceof Boolean)) {
            throw new IllegalArgumentException(field + " must be boolean");
        }
    }

    private static void validateOptionalId(Object value, String field) {
        if (value != null) {
            requireNonblank(value, field);
        }
    }
}
