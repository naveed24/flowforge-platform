package com.flowforge.workflow;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real PostgreSQL DDL smoke test, enabled only on CI's isolated database.
 * Regular local Maven tests need no database server.
 */
class PostgresFlywayMigrationTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "FLOWFORGE_PG_TEST_URL", matches = ".+")
    void migratesFromEmptyPostgresAndProvidesWorkflowAndExecutionColumns() throws Exception {
        String url = System.getenv("FLOWFORGE_PG_TEST_URL");
        if (!url.matches("jdbc:postgresql://(localhost|127\\.0\\.0\\.1):[0-9]+/flowforge_ci")) {
            throw new IllegalStateException("Migration smoke test requires localhost/flowforge_ci");
        }
        String username = System.getenv("FLOWFORGE_PG_TEST_USER");
        String password = System.getenv("FLOWFORGE_PG_TEST_PASSWORD");

        Flyway flyway = Flyway.configure().dataSource(url, username, password).load();
        flyway.migrate();
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("5");

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            assertThat(hasColumn(connection, "workflows", "schedule_type")).isTrue();
            assertThat(hasColumn(connection, "workflows", "cron_expression")).isTrue();
            assertThat(hasColumn(connection, "workflows", "schedule_timezone")).isTrue();
            assertThat(hasColumn(connection, "workflow_executions", "next_attempt_at")).isTrue();
            assertThat(hasColumn(connection, "workflow_executions", "idempotency_key")).isTrue();
            assertThat(hasIndex(connection, "uq_workflow_executions_idempotency")).isTrue();
        }
    }

    private boolean hasColumn(Connection connection, String table, String column) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
                        + "AND table_name = ? AND column_name = ?")) {
            query.setString(1, table);
            query.setString(2, column);
            try (ResultSet found = query.executeQuery()) {
                return found.next();
            }
        }
    }

    private boolean hasIndex(Connection connection, String name) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = ?")) {
            query.setString(1, name);
            try (ResultSet found = query.executeQuery()) {
                return found.next();
            }
        }
    }
}
