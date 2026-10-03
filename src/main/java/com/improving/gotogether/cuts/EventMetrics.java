package com.improving.gotogether.cuts;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

public final class EventMetrics implements EventMetricsMBean, AutoCloseable {
    private static final String DOMAIN = "com.improving.gotogether.events";

    private final String role;
    private final String identity;
    private final ObjectName objectName;
    private final MBeanServer server;
    private final AtomicLong recordsSent = new AtomicLong();
    private final AtomicLong sendErrors = new AtomicLong();
    private final AtomicLong recordsProcessed = new AtomicLong();
    private final AtomicLong processingErrors = new AtomicLong();

    private EventMetrics(String role, String identity) {
        this.role = role;
        this.identity = identity;
        server = ManagementFactory.getPlatformMBeanServer();
        try {
            objectName = new ObjectName(
                DOMAIN + ":identity=" + ObjectName.quote(identity) + ",role=" + role + ",type=EventMetrics"
            );
            server.registerMBean(this, objectName);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException exception) {
            throw new IllegalStateException("Could not register technician event metrics for " + identity, exception);
        }
    }

    public static EventMetrics producer(String clientId) {
        return new EventMetrics("producer", clientId);
    }

    public static EventMetrics consumer(String groupId) {
        return new EventMetrics("consumer", groupId);
    }

    public void recordSend() {
        recordsSent.incrementAndGet();
    }

    public void recordSendFailure() {
        sendErrors.incrementAndGet();
    }

    public void recordProcessed() {
        recordsProcessed.incrementAndGet();
    }

    public void recordProcessingFailure() {
        processingErrors.incrementAndGet();
    }

    public String role() {
        return role;
    }

    public String identity() {
        return identity;
    }

    @Override
    public long getRecordsSentTotal() {
        return recordsSent.get();
    }

    @Override
    public long getSendErrorsTotal() {
        return sendErrors.get();
    }

    @Override
    public long getRecordsProcessedTotal() {
        return recordsProcessed.get();
    }

    @Override
    public long getProcessingErrorsTotal() {
        return processingErrors.get();
    }

    @Override
    public String getRole() {
        return role;
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public void close() {
        try {
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        } catch (MBeanRegistrationException | javax.management.InstanceNotFoundException exception) {
            throw new IllegalStateException("Could not unregister technician event metrics for " + identity, exception);
        }
    }
}
