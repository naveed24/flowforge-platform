package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class TaskExecutionFenceServiceIntegrationTest {
    @Autowired private WorkflowService workflows;
    @Autowired private ExecutionService executions;
    @Autowired private ExecutionClaimService claims;
    @Autowired private ExecutionAttemptCompletionService completion;
    @Autowired private WorkflowExecutionRepository executionRepository;
    @Autowired private TaskExecutionCheckpointRepository checkpoints;
    @Autowired private TaskExecutionFenceService fences;

    @Test
    void admitsOnlyCurrentRunningTaskAndFencesDuplicateAndForgedCompletion() {
        long id = executions.enqueue(activeWorkflow()).getId();
        assertThat(fences.tryClaim(id, "send", 1)).isEmpty();
        assertThat(claims.claimQueuedById(id, 1)).isPresent();
        assertThatThrownBy(() -> fences.tryClaim(id, "not-a-task", 1))
                .isInstanceOf(IllegalArgumentException.class);

        var claim = fences.tryClaim(id, "send", 1).orElseThrow();
        assertThat(claim.idempotencyKey()).startsWith("ff-").hasSize(67);
        assertThat(fences.tryClaim(id, "send", 1)).isEmpty();
        assertThat(checkpoints.findByExecution_IdAndTaskKey(id, "send")).isPresent();
        assertThat(fences.succeed(new TaskExecutionFenceService.Claim(
                id, "send", 1, UUID.randomUUID(), claim.idempotencyKey()))).isFalse();
        assertThat(fences.succeed(claim)).isTrue();
        assertThat(fences.succeed(claim)).isFalse();
        assertThat(fences.tryClaim(id, "send", 1)).isEmpty();
        assertThat(checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow()
                .getState()).isEqualTo(TaskExecutionCheckpoint.State.SUCCEEDED);
    }

    @Test
    void uncertainExternalOutcomeFailsClosedAcrossRetries() {
        long id = executions.enqueue(activeWorkflow()).getId();
        assertThat(claims.claimQueuedById(id, 1)).isPresent();
        var uncertain = fences.tryClaim(id, "send", 1).orElseThrow();

        advanceToRetry(id, 1);
        assertThat(fences.tryClaim(id, "send", 2)).isEmpty();
        assertThat(fences.succeed(uncertain)).isFalse();
        assertThat(fences.markRetryable(uncertain)).isFalse();
        assertThat(checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow()
                .getState()).isEqualTo(TaskExecutionCheckpoint.State.IN_PROGRESS);
    }

    @Test
    void confirmedNoSideEffectMayRetryOnNextAttemptWithSameRemoteIdempotencyKey() {
        long id = executions.enqueue(activeWorkflow()).getId();
        assertThat(claims.claimQueuedById(id, 1)).isPresent();
        var first = fences.tryClaim(id, "send", 1).orElseThrow();
        assertThat(fences.markRetryable(first)).isTrue();
        assertThat(fences.markRetryable(first)).isFalse();
        assertThat(fences.tryClaim(id, "send", 1)).isEmpty();

        advanceToRetry(id, 1);
        var second = fences.tryClaim(id, "send", 2).orElseThrow();
        assertThat(second.token()).isNotEqualTo(first.token());
        assertThat(second.idempotencyKey()).isEqualTo(first.idempotencyKey());
        assertThat(fences.succeed(first)).isFalse();
        assertThat(fences.succeed(second)).isTrue();
        assertThat(fences.tryClaim(id, "send", 2)).isEmpty();
        assertThat(checkpoints.findByExecution_IdAndTaskKey(id, "send").orElseThrow()
                .getAttemptNumber()).isEqualTo(2);
    }

    @Test
    void cancelledExecutionRejectsAdmissionAndCompletion() {
        long id = executions.enqueue(activeWorkflow()).getId();
        var running = claims.claimQueuedById(id, 1).orElseThrow();
        var claim = fences.tryClaim(id, "send", 1).orElseThrow();
        executions.cancel(id, running.getVersion());
        assertThat(fences.succeed(claim)).isFalse();
        assertThat(fences.tryClaim(id, "other", 1)).isEmpty();
    }

    private void advanceToRetry(long id, int attempt) {
        assertThat(completion.fail(id, attempt)).isTrue();
        var waiting = executions.get(id);
        assertThat(executionRepository.requeueDueRetry(id, ExecutionStatus.RETRY_WAIT,
                ExecutionStatus.QUEUED, waiting.getNextAttemptAt())).isEqualTo(1);
        assertThat(claims.claimQueuedById(id, attempt + 1)).isPresent();
    }

    private long activeWorkflow() {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "fenced-task-" + UUID.randomUUID(), "Side-effect fencing integration test",
                List.of(new WorkflowDtos.TaskRequest("send", TaskType.HTTP, Set.of()),
                        new WorkflowDtos.TaskRequest("other", TaskType.NOOP, Set.of("send")))));
        return workflows.activate(created.id(), created.version()).id();
    }
}
