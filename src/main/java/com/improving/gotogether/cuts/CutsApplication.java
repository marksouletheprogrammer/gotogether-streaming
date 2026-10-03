package com.improving.gotogether.cuts;

public final class CutsApplication {
    private CutsApplication() {
    }

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        if (args.length == 1) {
            switch (args[0]) {
                case "lag-exporter" -> ConsumerLagExporter.runFromEnvironment();
                case "event-producer" -> EventProducer.runFromEnvironment();
                case "event-consumer" -> EventConsumerService.runFromEnvironment();
                case "event-outcome-exporter" -> EventOutcomeMetricsExporter.runFromEnvironment();
                default -> throw new IllegalArgumentException("Unknown application role: " + args[0]);
            }
            return;
        }
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected role and mode arguments");
        }
        switch (args[0]) {
            case "producer" -> runProducer(args[1]);
            case "consumer" -> runConsumer(args[1]);
            case "lag-exporter" -> ConsumerLagExporter.runFromEnvironment();
            default -> throw new IllegalArgumentException("Unknown application role: " + args[0]);
        }
    }

    private static void runProducer(String mode) {
        switch (mode) {
            case "transactional" -> CutProducer.run(true);
            case "at-least-once" -> CutProducer.run(false);
            default -> throw new IllegalArgumentException("Unknown producer mode: " + mode);
        }
    }

    private static void runConsumer(String mode) {
        switch (mode) {
            case "transactional" -> TransactionalCutService.runFromEnvironment();
            case "idempotent" -> IdempotentCutService.runFromEnvironment();
            default -> throw new IllegalArgumentException("Unknown consumer mode: " + mode);
        }
    }
}
