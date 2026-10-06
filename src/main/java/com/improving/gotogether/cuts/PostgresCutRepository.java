package com.improving.gotogether.cuts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;

public final class PostgresCutRepository implements CutRecordRepository {
    public enum Table {
        TRANSACTIONAL,
        IDEMPOTENT
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TRANSACTIONAL_UPSERT = "INSERT INTO mill_cuts_transactional_writes (event_id, payload) VALUES (?, ?::jsonb) "
        + "ON CONFLICT (event_id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()";
    private static final String IDEMPOTENT_UPSERT = "INSERT INTO mill_cuts_idempotent_writes (event_id, payload) VALUES (?, ?::jsonb) "
        + "ON CONFLICT (event_id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String sql;

    public PostgresCutRepository(String jdbcUrl, String username, String password, Table table) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        sql = table == Table.IDEMPOTENT ? IDEMPOTENT_UPSERT : TRANSACTIONAL_UPSERT;
    }

    @Override
    public void insert(GenericRecord record) {
        CutSchemas.validateCanonicalCut(record);
        String eventId = record.get("event_id").toString();
        String payload = serialize(CutSchemas.toMap(record));
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, eventId);
                statement.setString(2, payload);
                statement.executeUpdate();
                connection.commit();
            } catch (SQLException exception) {
                rollback(connection, exception);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist cut " + eventId, exception);
        }
    }

    private static String serialize(Map<String, Object> value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Could not encode cut database payload", exception);
        }
    }

    private static void rollback(Connection connection, SQLException failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
        throw new IllegalStateException("Could not commit cut database row", failure);
    }
}
