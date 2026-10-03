package com.improving.gotogether.cuts;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;

public final class SchemaRegistryAvroCodec implements AutoCloseable {
    private final KafkaAvroSerializer serializer;
    private final KafkaAvroDeserializer deserializer;

    public SchemaRegistryAvroCodec(String registryUrl, SchemaRegistryClient schemaRegistryClient) {
        Map<String, Object> configuration = Map.of(
            "schema.registry.url", registryUrl,
            "auto.register.schemas", false,
            "specific.avro.reader", false
        );
        serializer = new KafkaAvroSerializer(schemaRegistryClient);
        serializer.configure(configuration, false);
        deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        deserializer.configure(configuration, false);
    }

    public byte[] serialize(String topic, GenericRecord record) {
        CutSchemas.validateCanonicalCut(record);
        return serializer.serialize(topic, record);
    }

    public GenericRecord deserialize(String topic, byte[] value) {
        Object decoded = deserializer.deserialize(topic, value);
        if (!(decoded instanceof GenericRecord record)) {
            throw new IllegalArgumentException("Kafka value is not an Avro record");
        }
        CutSchemas.validateCanonicalCut(record);
        return record;
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }
}
