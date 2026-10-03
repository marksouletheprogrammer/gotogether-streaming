package com.improving.gotogether.cuts;

import org.apache.avro.generic.GenericRecord;

public interface EventStateRepository {
    boolean isProcessed(String eventId);

    boolean applyIfUnprocessed(GenericRecord event);
}
