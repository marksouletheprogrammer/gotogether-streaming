package com.improving.gotogether.cuts;

import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CutProducerConfigTest {
    @Test
    void configuresDistinctProducerClientsWithDifferentDeliveryGuarantees() {
        Properties transactional = CutProducerConfig.transactionalProducer(
            "broker:29092", "mill-cuts-transactional-producer", "http://schema-registry:8081"
        );
        Properties replay = CutProducerConfig.atLeastOnceProducer(
            "broker:29092", "mill-cuts-at-least-once-producer", "http://schema-registry:8081"
        );

        assertNotEquals(
            transactional.get(ProducerConfig.CLIENT_ID_CONFIG),
            replay.get(ProducerConfig.CLIENT_ID_CONFIG)
        );
        assertEquals(true, transactional.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(false, replay.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", transactional.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("all", replay.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(StringSerializer.class, transactional.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(KafkaAvroSerializer.class, transactional.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("http://schema-registry:8081", transactional.get("schema.registry.url"));
        assertEquals(transactional.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), replay.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
}
