package com.improving.gotogether.cuts;

import java.time.Instant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class CutRecordGenerator {
    private static final Instant START_TIME = Instant.parse("2026-03-04T10:15:00Z");
    private static final String TOOL_INSTANCE_ID = "cutter-1";

    public GenericRecord generate(int cutIndex) {
        if (cutIndex < 1) {
            throw new IllegalArgumentException("cutIndex must be positive");
        }

        Schema schema = CutSchemas.cutSchema();
        GenericRecord record = new GenericData.Record(schema);
        record.put("schema_version", "demo-1");
        record.put("event_id", "cut-" + TOOL_INSTANCE_ID + "-" + String.format("%03d", cutIndex));
        record.put("kind", "cut");
        record.put("occurred_at", START_TIME.plusSeconds(2L * (cutIndex - 1)).toString());
        record.put("machine_id", "mill-1");
        record.put("tool_instance_id", TOOL_INSTANCE_ID);

        Schema payloadSchema = schema.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("cut_index", cutIndex);
        payload.put("vibration_rms_g", 0.12 + (cutIndex % 10) * 0.001);
        payload.put("spindle_current_a", 11.0 + (cutIndex % 5) * 0.1);
        payload.put("coolant_flow_l_min", 12.0);
        record.put("payload", payload);

        CutSchemas.validateCanonicalCut(record);
        return record;
    }
}
