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

@SpringBootTest
@ActiveProfiles("test")
class ExecutionAttemptCompletionIntegrationTest {

    @Autowired private WorkflowService workflowService;
    @Autowired private ExecutionService executionService;
    @Autowired private ExecutionClaimService claims;
    @Autowired private ExecutionAttemptCompletionService completion;
    @Autowired private WorkflowExecutionRepository repository;

    @Test
    void targetedClaimAndDuplicateDeliveryAreSafe() {
        long workflowId = activeWorkflow();
        var first = executionService.enqueue(workflowId);
        var second = executionService.enqueue(workflowId);
        var claimed = claims.claimQueuedById(second.getId()).orElseThrow();

        assertThat(claimed.getAttemptCount()).isEqualTo(1);
        assertThat(claims.claimQueuedById(second.getId())).isEmpty();
        assertThat(executionService.get(first.getId()).getStatus()).isEqualTo(ExecutionStatus.QUEUED);
        assertThat(completion.succeed(second.getId(), 0)).isFalse();
        assertThat(completion.succeed(second.getId(), 1)).isTrue();
        assertThat(completion.succeed(second.getId(), 1)).isFalse();

        var done = executionService.get(second.getId());
        assertThat(done.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(done.getFinishedAt()).isNotNull();
        assertThat(done.getVersion()).isGreaterThan(claimed.getVersion());
    }

    @Test
    void staleAttemptCannotFinishLaterRetry() {
        long id = executionService.enqueue(activeWorkflow()).getId();
        assertThat(claims.claimQueuedById(id).orElseThrow().getAttemptCount()).isEqualTo(1);
        assertThat(completion.fail(id, 1)).isTrue();
        var waiting = executionService.get(id);
        assertThat(waiting.getStatus()).isEqualTo(ExecutionStatus.RETRY_WAIT);
        assertThat(waiting.getNextAttemptAt()).isNotNull();
        assertThat(waiting.getFinishedAt()).isNull();
        assertThat(completion.fail(id, 1)).isFalse();

        // Simulate a due retry through the scheduler's conditional requeue.
        assertThat(repository.requeueDueRetry(id, ExecutionStatus.RETRY_WAIT,
                ExecutionStatus.QUEUED, waiting.getNextAttemptAt())).isEqualTo(1);
        assertThat(claims.claimQueuedById(id).orElseThrow().getAttemptCount()).isEqualTo(2);

        assertThat(completion.succeed(id, 1)).isFalse();
        assertThat(completion.fail(id, 1)).isFalse();
        assertThat(executionService.get(id).getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(completion.succeed(id, 2)).isTrue();
        assertThat(executionService.get(id).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
    }

    @Test
    void cancellationAndRetryExhaustionCannotBeOverwritten() {
        long cancelledId = executionService.enqueue(activeWorkflow()).getId();
        var claimed = claims.claimQueuedById(cancelledId).orElseThrow();
        executionService.cancel(cancelledId, claimed.getVersion());
        assertThat(completion.succeed(cancelledId, 1)).isFalse();
        assertThat(completion.fail(cancelledId, 1)).isFalse();
        assertThat(claims.claimQueuedById(cancelledId)).isEmpty();

        long id = executionService.enqueue(activeWorkflow()).getId();
        for (int attempt = 1; attempt <= RetryBackoffPolicy.DEFAULT_MAX_ATTEMPTS; attempt++) {
            assertThat(claims.claimQueuedById(id).orElseThrow().getAttemptCount()).isEqualTo(attempt);
            assertThat(completion.fail(id, attempt)).isTrue();
            var updated = executionService.get(id);
            if (attempt < RetryBackoffPolicy.DEFAULT_MAX_ATTEMPTS) {
                assertThat(updated.getStatus()).isEqualTo(ExecutionStatus.RETRY_WAIT);
                assertThat(repository.requeueDueRetry(id, ExecutionStatus.RETRY_WAIT,
                        ExecutionStatus.QUEUED, updated.getNextAttemptAt())).isEqualTo(1);
            } else {
                assertThat(updated.getStatus()).isEqualTo(ExecutionStatus.FAILED);
                assertThat(updated.getNextAttemptAt()).isNull();
                assertThat(updated.getFinishedAt()).isNotNull();
                assertThat(completion.fail(id, attempt)).isFalse();
                assertThat(claims.claimQueuedById(id)).isEmpty();
            }
        }
    }

    private long activeWorkflow() {
        var created = workflowService.create(new WorkflowDtos.CreateWorkflowRequest(
                "fenced-worker-" + UUID.randomUUID(),
                "Attempt-fenced worker completion integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));
        return workflowService.activate(created.id(), created.version()).id();
    }
}
