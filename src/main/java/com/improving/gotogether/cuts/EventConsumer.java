package com.improving.gotogether.cuts;

import java.util.function.DoubleSupplier;

import org.apache.avro.generic.GenericRecord;

public final class EventConsumer {
    public enum Outcome {
        PROCESSED,
        DEAD_LETTERED,
        SILENTLY_DROPPED,
        ALREADY_PROCESSED
    }

    private static final String SIMULATED_FAILURE = "simulated processing failure";

    private final EventStateRepository repository;
    private final EventDeadLetterWriter deadLetterWriter;
    private final double dlqProbability;
    private final double silentDropProbability;
    private final DoubleSupplier random;

    public EventConsumer(
        EventStateRepository repository,
        EventDeadLetterWriter deadLetterWriter,
        double dlqProbability,
        DoubleSupplier random
    ) {
        this(repository, deadLetterWriter, dlqProbability, 0.0, random);
    }

    public EventConsumer(
        EventStateRepository repository,
        EventDeadLetterWriter deadLetterWriter,
        double dlqProbability,
        double silentDropProbability,
        DoubleSupplier random
    ) {
        if (!Double.isFinite(dlqProbability) || dlqProbability < 0 || dlqProbability > 1) {
            throw new IllegalArgumentException("dlqProbability must be between zero and one");
        }
        if (!Double.isFinite(silentDropProbability) || silentDropProbability < 0 || silentDropProbability > 1) {
            throw new IllegalArgumentException("silentDropProbability must be between zero and one");
        }
        if (dlqProbability + silentDropProbability > 1) {
            throw new IllegalArgumentException("dlqProbability + silentDropProbability must not exceed one");
        }
        this.repository = repository;
        this.deadLetterWriter = deadLetterWriter;
        this.dlqProbability = dlqProbability;
        this.silentDropProbability = silentDropProbability;
        this.random = random;
    }

    public Outcome process(GenericRecord event, Runnable commitOffset) {
        EventSchemas.validateCanonicalEvent(event);
        String eventId = event.get("event_id").toString();
        if (repository.isProcessed(eventId)) {
            commitOffset.run();
            return Outcome.ALREADY_PROCESSED;
        }

        // Single random draw for three-way selection
        double randomValue = random.getAsDouble();

        // DLQ failure: [0, dlqProbability)
        if (randomValue < dlqProbability) {
            deadLetterWriter.publish(event, SIMULATED_FAILURE);
            commitOffset.run();
            return Outcome.DEAD_LETTERED;
        }

        // Silent drop: [dlqProbability, dlqProbability + silentDropProbability)
        if (randomValue < dlqProbability + silentDropProbability) {
            commitOffset.run();
            return Outcome.SILENTLY_DROPPED;
        }

        // Success: [dlqProbability + silentDropProbability, 1.0)
        boolean processed = repository.applyIfUnprocessed(event);
        commitOffset.run();
        return processed ? Outcome.PROCESSED : Outcome.ALREADY_PROCESSED;
    }
}
