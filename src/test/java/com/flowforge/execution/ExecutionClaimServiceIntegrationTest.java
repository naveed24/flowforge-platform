package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class ExecutionClaimServiceIntegrationTest {

    @Autowired private WorkflowService workflowService;
    @Autowired private ExecutionService executionService;
    @Autowired private ExecutionClaimService claimService;
    @Autowired private WorkflowExecutionRepository executionRepository;

    @BeforeEach
    void clearPreviousExecutions() {
        // H2 test contexts are reused by other integration test classes.
        executionRepository.deleteAll();
    }

    @Test
    void claimsQueuedExecutionsOldestFirstOnlyOnce() {
        Long workflowId = activeWorkflow();
        var first = executionService.enqueue(workflowId);
        var second = executionService.enqueue(workflowId);

        var claimedFirst = claimService.claimNextQueued().orElseThrow();
        assertThat(claimedFirst.getId()).isEqualTo(first.getId());
        assertThat(claimedFirst.getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(claimedFirst.getAttemptCount()).isEqualTo(1);
        assertThat(claimedFirst.getStartedAt()).isNotNull();
        assertThat(claimedFirst.getVersion()).isGreaterThan(first.getVersion());

        var claimedSecond = claimService.claimNextQueued().orElseThrow();
        assertThat(claimedSecond.getId()).isEqualTo(second.getId());
        assertThat(claimService.claimNextQueued()).isEmpty();
        assertThat(executionService.get(first.getId()).getAttemptCount()).isEqualTo(1);
    }

    @Test
    void skipsCancelledAndRetryWaitingExecutions() {
        Long workflowId = activeWorkflow();
        var retrying = executionService.enqueue(workflowId);
        executionService.start(retrying.getId());
        executionService.fail(retrying.getId());

        var cancelled = executionService.enqueue(workflowId);
        executionService.cancel(cancelled.getId(), cancelled.getVersion());

        assertThat(claimService.claimNextQueued()).isEmpty();
        assertThat(executionService.get(retrying.getId()).getStatus())
                .isEqualTo(ExecutionStatus.RETRY_WAIT);
        assertThat(executionService.get(cancelled.getId()).getStatus())
                .isEqualTo(ExecutionStatus.CANCELLED);
    }

    @Test
    void competingWorkersCannotClaimSameExecution() throws Exception {
        Long workflowId = activeWorkflow();
        Long executionId = executionService.enqueue(workflowId).getId();
        var ready = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            Callable<Long> worker = () -> {
                ready.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for competing worker");
                }
                return claimService.claimNextQueued().map(WorkflowExecution::getId).orElse(null);
            };
            var one = pool.submit(worker);
            var two = pool.submit(worker);
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            release.countDown();

            var claimed = java.util.Arrays.asList(
                    one.get(15, TimeUnit.SECONDS), two.get(15, TimeUnit.SECONDS));
            assertThat(claimed.stream().filter(id -> id != null).toList())
                    .containsExactly(executionId);
            assertThat(executionService.get(executionId).getAttemptCount()).isEqualTo(1);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    private Long activeWorkflow() {
        var created = workflowService.create(new WorkflowDtos.CreateWorkflowRequest(
                "worker-claim-" + UUID.randomUUID(),
                "Atomic worker claim integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));
        return workflowService.activate(created.id(), created.version()).id();
    }
}
