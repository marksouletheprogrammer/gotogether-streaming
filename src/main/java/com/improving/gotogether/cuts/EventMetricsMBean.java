package com.improving.gotogether.cuts;

public interface EventMetricsMBean {
    long getRecordsSentTotal();

    long getSendErrorsTotal();

    long getRecordsProcessedTotal();

    long getProcessingErrorsTotal();

    String getRole();

    String getIdentity();
}
