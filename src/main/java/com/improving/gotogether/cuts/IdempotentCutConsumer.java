package com.improving.gotogether.cuts;

import org.apache.avro.generic.GenericRecord;

public final class IdempotentCutConsumer {
    private final CutRecordRepository repository;

    public IdempotentCutConsumer(CutRecordRepository repository) {
        this.repository = repository;
    }

    public void process(GenericRecord record, Runnable commitOffset) {
        CutSchemas.validateCanonicalCut(record);
        repository.insert(record);
        commitOffset.run();
    }
}
