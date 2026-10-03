package com.improving.gotogether.cuts;

import java.util.function.DoubleSupplier;

import org.apache.avro.generic.GenericRecord;

public final class EventConsumer {
    public enum Outcome {
        PROCESSED,
        DEAD_LETTERED,
        ALREADY_PROCESSED
    }

    private static final String SIMULATED_FAILURE = "simulated processing failure";

    private final EventStateRepository repository;
    private final EventDeadLetterWriter deadLetterWriter;
    private final double failureProbability;
    private final DoubleSupplier random;

    public EventConsumer(
        EventStateRepository repository,
        EventDeadLetterWriter deadLetterWriter,
        double failureProbability,
        DoubleSupplier random
    ) {
        if (!Double.isFinite(failureProbability) || failureProbability < 0 || failureProbability > 1) {
            throw new IllegalArgumentException("failureProbability must be between zero and one");
        }
        this.repository = repository;
        this.deadLetterWriter = deadLetterWriter;
        this.failureProbability = failureProbability;
        this.random = random;
    }

    public Outcome process(GenericRecord event, Runnable commitOffset) {
        EventSchemas.validateCanonicalEvent(event);
        String eventId = event.get("event_id").toString();
        if (repository.isProcessed(eventId)) {
            commitOffset.run();
            return Outcome.ALREADY_PROCESSED;
        }
        if (random.getAsDouble() < failureProbability) {
            deadLetterWriter.publish(event, SIMULATED_FAILURE);
            commitOffset.run();
            return Outcome.DEAD_LETTERED;
        }
        boolean processed = repository.applyIfUnprocessed(event);
        commitOffset.run();
        return processed ? Outcome.PROCESSED : Outcome.ALREADY_PROCESSED;
    }
}
