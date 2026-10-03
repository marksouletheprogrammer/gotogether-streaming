package com.improving.gotogether.cuts;

public interface CutMetricsMBean {
    String getRole();

    String getIdentity();

    long getRecordsSentTotal();

    long getSendErrorsTotal();

    long getRecordsProcessedTotal();

    long getProcessingErrorsTotal();
}
