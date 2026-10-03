package com.improving.gotogether.cuts;

import org.apache.avro.Schema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CutRecordGeneratorTest {
    @Test
    void generatesTheSameCanonicalCutForTheSameIndex() {
        CutRecordGenerator generator = new CutRecordGenerator();

        assertEquals(generator.generate(1), generator.generate(1));
        assertNotEquals(generator.generate(1), generator.generate(2));
    }

    @Test
    void cutAvroSchemaRetainsCanonicalEnvelopeAndPayloadFields() {
        Schema schema = CutSchemas.cutSchema();

        assertEquals("com.improving.gotogether.cuts", schema.getNamespace());
        assertEquals("event_id", schema.getField("event_id").name());
        assertEquals("record", schema.getField("payload").schema().getType().getName().toLowerCase());
        assertEquals("cut_index", schema.getField("payload").schema().getField("cut_index").name());
    }

    @Test
    void rejectsValuesThatViolateTheCanonicalCutContract() {
        CutRecordGenerator generator = new CutRecordGenerator();
        GenericRecord emptyId = generator.generate(1);
        emptyId.put("event_id", " ");
        GenericRecord malformedTimestamp = generator.generate(1);
        malformedTimestamp.put("occurred_at", "not-a-timestamp");
        GenericRecord wrongKind = generator.generate(1);
        wrongKind.put("kind", "event");
        GenericRecord negativeMeasurement = generator.generate(1);
        ((GenericRecord) negativeMeasurement.get("payload")).put("coolant_flow_l_min", -1.0);

        assertThrows(IllegalArgumentException.class, () -> CutSchemas.validateCanonicalCut(emptyId));
        assertThrows(IllegalArgumentException.class, () -> CutSchemas.validateCanonicalCut(malformedTimestamp));
        assertThrows(IllegalArgumentException.class, () -> CutSchemas.validateCanonicalCut(wrongKind));
        assertThrows(IllegalArgumentException.class, () -> CutSchemas.validateCanonicalCut(negativeMeasurement));
    }

    @Test
    void generatedRecordsRoundTripThroughAvroAndSchemaRegistry() throws Exception {
        GenericRecord record = new CutRecordGenerator().generate(1);
        Schema schema = CutSchemas.cutSchema();
        byte[] encoded = CutSchemas.encode(record, schema);
        assertEquals(record, CutSchemas.decode(encoded, schema));

        String topic = "mill-cuts-transactional-source";
        MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
        registry.register(topic + "-value", schema);
        try (SchemaRegistryAvroCodec codec = new SchemaRegistryAvroCodec("mock://cut-test", registry)) {
            byte[] wireValue = codec.serialize(topic, record);

            assertEquals(0, wireValue[0]);
            assertEquals(record, codec.deserialize(topic, wireValue));
        }
    }
}
