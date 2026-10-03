package com.improving.gotogether.cuts;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public final class CutSchemas {
    private static final String SCHEMA_RESOURCE = "/avro/cut-record.avsc";
    private static final Schema CUT_SCHEMA = loadSchema();

    private CutSchemas() {
    }

    public static Schema cutSchema() {
        return CUT_SCHEMA;
    }

    public static byte[] encode(GenericRecord record, Schema schema) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not encode cut Avro record", exception);
        }
    }

    public static GenericRecord decode(byte[] encoded, Schema schema) {
        try {
            var decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(encoded), null);
            return new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
        } catch (IOException exception) {
            throw new IllegalArgumentException("Could not decode cut Avro record", exception);
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

    public static void validateCanonicalCut(GenericRecord record) {
        if (!CUT_SCHEMA.equals(record.getSchema())) {
            throw new IllegalArgumentException("Cut record does not use the canonical Avro schema");
        }
        requireString(record.get("schema_version"), "demo-1", "schema_version");
        requireNonblank(record.get("event_id"), "event_id");
        requireString(record.get("kind"), "cut", "kind");
        requireTimestamp(record.get("occurred_at"));
        requireString(record.get("machine_id"), "mill-1", "machine_id");
        requireNonblank(record.get("tool_instance_id"), "tool_instance_id");

        Object payloadValue = record.get("payload");
        if (!(payloadValue instanceof GenericRecord payload)) {
            throw new IllegalArgumentException("Cut payload must be an Avro record");
        }
        Object cutIndex = payload.get("cut_index");
        if (!(cutIndex instanceof Integer index) || index < 1) {
            throw new IllegalArgumentException("cut_index must be a positive integer");
        }
        requireNonnegativeNumber(payload.get("vibration_rms_g"), "vibration_rms_g");
        requireNonnegativeNumber(payload.get("spindle_current_a"), "spindle_current_a");
        requireNonnegativeNumber(payload.get("coolant_flow_l_min"), "coolant_flow_l_min");
    }

    private static Schema loadSchema() {
        try (InputStream input = CutSchemas.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("Missing canonical cut Avro schema: " + SCHEMA_RESOURCE);
            }
            return new Schema.Parser().parse(input);
        } catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static void requireString(Object value, String expected, String field) {
        if (!expected.equals(value == null ? null : value.toString())) {
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

    private static void requireNonnegativeNumber(Object value, String field) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(field + " must be numeric");
        }
        double measurement = number.doubleValue();
        if (!Double.isFinite(measurement) || measurement < 0) {
            throw new IllegalArgumentException(field + " must be a finite nonnegative number");
        }
    }
}
