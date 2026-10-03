package com.improving.gotogether.cuts;

import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventProducerConfigTest {
    @Test
    void configuresASeparateAtLeastOnceAvroProducerForEvents() {
        Properties properties = EventProducerConfig.create(
            "broker:29092", "mill-tool-events-producer", "http://schema-registry:8081"
        );

        assertEquals("broker:29092", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("mill-tool-events-producer", properties.get(ProducerConfig.CLIENT_ID_CONFIG));
        assertEquals(false, properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", properties.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(StringSerializer.class, properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(KafkaAvroSerializer.class, properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("http://schema-registry:8081", properties.get("schema.registry.url"));
        assertEquals(false, properties.get("auto.register.schemas"));
    }
}
