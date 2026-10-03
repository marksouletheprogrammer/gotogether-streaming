package com.improving.gotogether.cuts;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class EventDeadLetterWriterTest {
    @Test
    void writesTheOriginalEventAndUtf8ErrorHeaderToTheDlqTopic() {
        GenericRecord event = new EventRecordGenerator().generate(1);
        AtomicReference<ProducerRecord<String, GenericRecord>> sent = new AtomicReference<>();
        KafkaEventDeadLetterWriter writer = new KafkaEventDeadLetterWriter("mill-tool-events-dlq", sent::set);

        writer.publish(event, "simulated processing failure");

        ProducerRecord<String, GenericRecord> record = sent.get();
        assertNotNull(record);
        assertEquals("mill-tool-events-dlq", record.topic());
        assertEquals(event.get("event_id").toString(), record.key());
        assertSame(event, record.value());
        Header error = record.headers().lastHeader("error");
        assertNotNull(error);
        assertEquals("simulated processing failure", new String(error.value(), StandardCharsets.UTF_8));
    }
}
