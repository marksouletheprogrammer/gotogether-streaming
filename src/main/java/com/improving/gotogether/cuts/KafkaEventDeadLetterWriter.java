package com.improving.gotogether.cuts;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

public final class KafkaEventDeadLetterWriter implements EventDeadLetterWriter {
    private final String topic;
    private final Consumer<ProducerRecord<String, GenericRecord>> sender;

    public KafkaEventDeadLetterWriter(String topic, Consumer<ProducerRecord<String, GenericRecord>> sender) {
        this.topic = topic;
        this.sender = sender;
    }

    @Override
    public void publish(GenericRecord event, String error) {
        String eventId = event.get("event_id").toString();
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, eventId, event);
        record.headers().add(new RecordHeader("error", error.getBytes(StandardCharsets.UTF_8)));
        sender.accept(record);
    }
}
