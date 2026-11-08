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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("13");

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            assertThat(hasColumn(connection, "workflows", "schedule_type")).isTrue();
            assertThat(hasColumn(connection, "workflows", "cron_expression")).isTrue();
            assertThat(hasColumn(connection, "workflows", "schedule_timezone")).isTrue();
            assertThat(hasColumn(connection, "workflow_executions", "next_attempt_at")).isTrue();
            assertThat(hasColumn(connection, "workflow_executions", "idempotency_key")).isTrue();
            assertThat(hasIndex(connection, "uq_workflow_executions_idempotency")).isTrue();
            assertThat(hasIndex(connection, "idx_workflow_executions_history")).isTrue();
            assertThat(hasIndex(connection, "idx_workflow_executions_history_status")).isTrue();
            assertThat(hasColumn(connection, "execution_dispatch_outbox", "execution_id")).isTrue();
            assertThat(hasColumn(connection, "execution_dispatch_outbox", "attempt_number")).isTrue();
            assertThat(hasColumn(connection, "execution_dispatch_outbox", "published_at")).isTrue();
            assertThat(hasIndex(connection, "uq_execution_dispatch_outbox_attempt")).isTrue();
            assertThat(hasIndex(connection, "idx_execution_dispatch_outbox_pending")).isTrue();
            assertThat(hasColumn(connection, "execution_dispatch_outbox", "claim_token")).isTrue();
            assertThat(hasColumn(connection, "execution_dispatch_outbox", "claimed_until")).isTrue();
            assertThat(hasIndex(connection, "idx_execution_dispatch_outbox_claimable")).isTrue();
            assertThat(hasColumn(connection, "execution_task_checkpoints", "claim_token")).isTrue();
            assertThat(hasColumn(connection, "execution_task_checkpoints", "attempt_number")).isTrue();
            assertThat(hasIndex(connection, "uq_execution_task_checkpoints_task")).isTrue();
            assertThat(hasIndex(connection, "idx_execution_task_checkpoints_state")).isTrue();
            assertThat(hasColumn(connection, "task_checkpoint_reconciliation_audit", "checkpoint_id")).isTrue();
            assertThat(hasColumn(connection, "task_checkpoint_reconciliation_audit", "previous_claim_token")).isTrue();
            assertThat(hasColumn(connection, "task_checkpoint_reconciliation_audit", "evidence_reference")).isTrue();
            assertThat(hasIndex(connection, "idx_task_checkpoint_reconciliation_audit_checkpoint")).isTrue();
            assertThat(hasIndex(connection, "uq_task_checkpoint_reconciliation_claim")).isTrue();
            assertThat(hasConstraint(connection, "task_checkpoint_reconciliation_audit",
                    "chk_task_reconciliation_previous_attempt")).isTrue();
            assertThat(hasConstraint(connection, "task_checkpoint_reconciliation_audit",
                    "chk_task_reconciliation_evidence_not_blank")).isTrue();
            assertThat(hasConstraint(connection, "task_checkpoint_reconciliation_audit",
                    "chk_task_reconciliation_evidence_no_controls")).isTrue();
            assertEvidenceControlConstraint(connection);
        }
    }

    /** Exercise the real PostgreSQL CHECK without requiring a workflow fixture. */
    private void assertEvidenceControlConstraint(Connection connection) throws SQLException {
        // LIKE INCLUDING CONSTRAINTS copies CHECK constraints, but not foreign keys.
        try (java.sql.Statement statement = connection.createStatement()) {
            statement.execute("CREATE TEMP TABLE reconciliation_evidence_probe "
                    + "(LIKE task_checkpoint_reconciliation_audit INCLUDING CONSTRAINTS)");
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO reconciliation_evidence_probe
                    (id, checkpoint_id, previous_attempt, previous_claim_token,
                     resolved_state, evidence_reference, reconciled_at)
                VALUES (?, 1, 1, '00000000-0000-0000-0000-000000000001',
                        'SUCCEEDED', ?, CURRENT_TIMESTAMP)
                """)) {
            insert.setLong(1, 1);
            insert.setString(2, "ticket:verified-123");
            assertThat(insert.executeUpdate()).isEqualTo(1);

            insert.setLong(1, 2);
            insert.setString(2, "ticket:forged\nentry");
            assertThatThrownBy(insert::executeUpdate).isInstanceOf(SQLException.class);

            insert.setLong(1, 3);
            insert.setString(2, "ticket:forged\rentry");
            assertThatThrownBy(insert::executeUpdate).isInstanceOf(SQLException.class);
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
    private boolean hasConstraint(Connection connection, String table, String constraint) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT 1 FROM pg_constraint c
                  JOIN pg_class t ON t.oid = c.conrelid
                  JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE n.nspname = 'public' AND t.relname = ?
                   AND c.conname = ? AND c.contype = 'c' AND c.convalidated
                """)) {
            query.setString(1, table);
            query.setString(2, constraint);
            try (ResultSet found = query.executeQuery()) {
                return found.next();
            }
        }
    }

}
