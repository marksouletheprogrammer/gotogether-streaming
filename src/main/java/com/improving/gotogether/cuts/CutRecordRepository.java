package com.improving.gotogether.cuts;

import org.apache.avro.generic.GenericRecord;

@FunctionalInterface
public interface CutRecordRepository {
    void insert(GenericRecord record);
}
