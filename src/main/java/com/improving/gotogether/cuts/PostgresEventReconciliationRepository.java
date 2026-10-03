package com.improving.gotogether.cuts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class PostgresEventReconciliationRepository implements EventReconciliationRepository {
    private static final String INSERT = "INSERT INTO mill_tool_event_reconciliation (event_id) VALUES (?) "
        + "ON CONFLICT (event_id) DO NOTHING";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public PostgresEventReconciliationRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void ensureEventRow(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must be non-empty");
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
                statement.setString(1, eventId);
                statement.executeUpdate();
                connection.commit();
            } catch (SQLException exception) {
                rollback(connection, exception);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not commit event reconciliation row " + eventId, exception);
        }
    }

    private static void rollback(Connection connection, SQLException failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
        throw new IllegalStateException("Could not commit event reconciliation row", failure);
    }
}
