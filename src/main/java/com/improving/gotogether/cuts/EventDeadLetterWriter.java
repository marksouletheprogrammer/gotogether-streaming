package com.improving.gotogether.cuts;

import org.apache.avro.generic.GenericRecord;

public interface EventDeadLetterWriter {
    void publish(GenericRecord event, String error);
}
