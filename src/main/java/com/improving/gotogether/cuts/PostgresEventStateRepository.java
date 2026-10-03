package com.improving.gotogether.cuts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;

public final class PostgresEventStateRepository implements EventStateRepository {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String IS_PROCESSED = "SELECT processed FROM mill_tool_event_reconciliation WHERE event_id = ?";
    private static final String LOCK_RECONCILIATION = IS_PROCESSED + " FOR UPDATE";
    private static final String UPSERT_STATE = "INSERT INTO mill_tool_event_state "
        + "(machine_id, tool_instance_id, event_id, payload, updated_at) VALUES (?, ?, ?, ?::jsonb, NOW()) "
        + "ON CONFLICT (machine_id, tool_instance_id) DO UPDATE SET event_id = EXCLUDED.event_id, "
        + "payload = EXCLUDED.payload, updated_at = NOW()";
    private static final String MARK_PROCESSED = "UPDATE mill_tool_event_reconciliation SET processed = TRUE "
        + "WHERE event_id = ? AND processed = FALSE";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public PostgresEventStateRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean isProcessed(String eventId) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement statement = connection.prepareStatement(IS_PROCESSED)) {
            statement.setString(1, eventId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Missing event reconciliation row " + eventId);
                }
                return result.getBoolean(1);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not read event reconciliation row " + eventId, exception);
        }
    }

    @Override
    public boolean applyIfUnprocessed(GenericRecord event) {
        EventSchemas.validateCanonicalEvent(event);
        String eventId = event.get("event_id").toString();
        Map<String, Object> values = EventSchemas.toMap(event);
        String payload = serialize(values);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            connection.setAutoCommit(false);
            try {
                if (isProcessed(connection, eventId)) {
                    connection.commit();
                    return false;
                }
                upsertState(connection, event, eventId, payload);
                markProcessed(connection, eventId);
                connection.commit();
                return true;
            } catch (SQLException exception) {
                rollback(connection, exception);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist event " + eventId, exception);
        }
        throw new IllegalStateException("Could not persist event " + eventId);
    }

    private static boolean isProcessed(Connection connection, String eventId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LOCK_RECONCILIATION)) {
            statement.setString(1, eventId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Missing event reconciliation row " + eventId);
                }
                return result.getBoolean(1);
            }
        }
    }

    private static void upsertState(Connection connection, GenericRecord event, String eventId, String payload)
        throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_STATE)) {
            statement.setString(1, event.get("machine_id").toString());
            statement.setString(2, event.get("tool_instance_id").toString());
            statement.setString(3, eventId);
            statement.setString(4, payload);
            statement.executeUpdate();
        }
    }

    private static void markProcessed(Connection connection, String eventId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(MARK_PROCESSED)) {
            statement.setString(1, eventId);
            if (statement.executeUpdate() != 1) {
                throw new SQLException("Could not mark event reconciliation row processed " + eventId);
            }
        }
    }

    private static String serialize(Map<String, Object> value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Could not encode event database payload", exception);
        }
    }

    private static void rollback(Connection connection, SQLException failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
        throw new IllegalStateException("Could not commit event target and reconciliation state", failure);
    }
}
