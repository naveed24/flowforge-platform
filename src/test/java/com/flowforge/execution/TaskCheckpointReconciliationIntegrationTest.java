package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class TaskCheckpointReconciliationIntegrationTest {
    @Autowired WorkflowService workflows;
    @Autowired ExecutionService executions;
    @Autowired ExecutionClaimService claims;
    @Autowired ExecutionAttemptCompletionService completion;
    @Autowired WorkflowExecutionRepository executionRepository;
    @Autowired TaskExecutionFenceService fences;
    @Autowired TaskExecutionCheckpointRepository checkpoints;
    @Autowired TaskCheckpointReconciliationAuditRepository audit;
    @Autowired TaskCheckpointReconciliationService reconciliation;
    @Autowired JdbcTemplate jdbc;

    @Test
    void verifiedRemoteSuccessBlocksReplayAndWritesSingleAudit() {
        long id = createRunning();
        var claim = fences.tryClaim(id, "send", 1).orElseThrow();
        var row = checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow();
        assertThat(reconciliation.reconcile(id, "send", 1, UUID.randomUUID(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED, "remote:123")).isFalse();
        assertThat(audit.countByCheckpoint_Id(row.getId())).isZero();
        assertThat(reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED, "remote:123")).isTrue();
        assertThat(reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED, "remote:123")).isFalse();
        assertThat(audit.countByCheckpoint_Id(row.getId())).isEqualTo(1);
        assertThat(fences.tryClaim(id, "send", 1)).isEmpty();
        assertThat(fences.succeed(claim)).isFalse();
    }

    @Test
    void provenNonExecutionAllowsOnlyNextAttemptAndRejectsStaleReconciliation() {
        long id = createRunning();
        var claim = fences.tryClaim(id, "send", 1).orElseThrow();
        assertThatThrownBy(() -> reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_DID_NOT_EXECUTE, "  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(completion.fail(id, 1)).isTrue();
        assertThat(reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_DID_NOT_EXECUTE, "provider:absent")).isTrue();
        assertThat(fences.tryClaim(id, "send", 1)).isEmpty();
        var waiting = executions.get(id);
        assertThat(executionRepository.requeueDueRetry(id, ExecutionStatus.RETRY_WAIT,
                ExecutionStatus.QUEUED, waiting.getNextAttemptAt())).isEqualTo(1);
        assertThat(claims.claimQueuedById(id, 2)).isPresent();
        var next = fences.tryClaim(id, "send", 2).orElseThrow();
        assertThat(next.idempotencyKey()).isEqualTo(claim.idempotencyKey());
        assertThat(next.token()).isNotEqualTo(claim.token());
        assertThat(reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED, "late:proof")).isFalse();
        assertThat(checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow()
                .getState()).isEqualTo(TaskExecutionCheckpoint.State.IN_PROGRESS);
    }

    @Test
    void invalidEvidenceCannotAlterCheckpointOrCreateAudit() {
        long id = createRunning();
        var claim = fences.tryClaim(id, "send", 1).orElseThrow();
        var row = checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow();
        for (String invalid : List.of("  ", "ticket:123\nforged", "ticket:123\rforged",
                "ticket:\u200B123", "ticket:\u202E123", "x".repeat(513))) {
            assertThatThrownBy(() -> reconciliation.reconcile(id, "send", 1, claim.token(),
                    TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED, invalid))
                    .as("Invalid evidence: %s", invalid).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(audit.countByCheckpoint_Id(row.getId())).isZero();
        assertThat(checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow()
                .getState()).isEqualTo(TaskExecutionCheckpoint.State.IN_PROGRESS);
        assertThat(reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED,
                "  provider:receipt-123  ")).isTrue();
        assertThat(audit.countByCheckpoint_Id(row.getId())).isEqualTo(1);
    }

    @Test
    void databaseRejectsDuplicateReconciliationOfSameClaim() {
        long id = createRunning();
        var claim = fences.tryClaim(id, "send", 1).orElseThrow();
        var checkpoint = checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow();
        assertThat(reconciliation.reconcile(id, "send", 1, claim.token(),
                TaskCheckpointReconciliationService.VerifiedOutcome.REMOTE_SUCCEEDED,
                "provider:receipt")).isTrue();

        // Exercise the database constraint directly, not the service's state
        // guard. Use explicit VALUES to guarantee a duplicate insert attempt.
        assertThat(audit.countByCheckpoint_Id(checkpoint.getId())).isEqualTo(1);
        var existing = jdbc.queryForMap("""
                SELECT previous_attempt, previous_claim_token, resolved_state, reconciled_at
                  FROM task_checkpoint_reconciliation_audit
                 WHERE checkpoint_id = ?
                """, checkpoint.getId());
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO task_checkpoint_reconciliation_audit
                    (checkpoint_id, previous_attempt, previous_claim_token,
                     resolved_state, evidence_reference, reconciled_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """, checkpoint.getId(), existing.get("previous_attempt"),
                existing.get("previous_claim_token"), existing.get("resolved_state"),
                "duplicate:receipt", existing.get("reconciled_at")))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(audit.countByCheckpoint_Id(checkpoint.getId())).isEqualTo(1);
    }

    private long createRunning() {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "reconcile-" + UUID.randomUUID(), "Reconciliation test",
                List.of(new WorkflowDtos.TaskRequest("send", TaskType.HTTP, Set.of()))));
        long workflowId = workflows.activate(created.id(), created.version()).id();
        long id = executions.enqueue(workflowId).getId();
        assertThat(claims.claimQueuedById(id, 1)).isPresent();
        return id;
    }
}
