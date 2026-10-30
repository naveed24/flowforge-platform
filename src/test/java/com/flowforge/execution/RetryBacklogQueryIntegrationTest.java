package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class RetryBacklogQueryIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-10-30T08:00:00Z");

    @Autowired private WorkflowService workflows;
    @Autowired private ExecutionService executions;
    @Autowired private WorkflowExecutionRepository repository;
    @Autowired private ExecutionDispatchOutboxRepository outbox;
    @Autowired private RetryBacklogQuery query;

    @BeforeEach
    void clear() {
        outbox.deleteAllInBatch();
        repository.deleteAllInBatch();
    }

    @Test
    void countsAllWaitingRetriesIncludingExactDueBoundaryAndExcludesOtherStatuses() {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "retry-backlog-" + UUID.randomUUID(),
                "Retry backlog query integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.NOOP, Set.of()))));
        long workflowId = workflows.activate(created.id(), created.version()).id();

        var overdue = executions.enqueue(workflowId);
        var exactlyDue = executions.enqueue(workflowId);
        var future = executions.enqueue(workflowId);
        executions.enqueue(workflowId); // QUEUED is not part of the retry backlog.

        setWaiting(overdue.getId(), NOW.minusSeconds(90));
        setWaiting(exactlyDue.getId(), NOW);
        setWaiting(future.getId(), NOW.plusSeconds(30));

        assertThat(query.read(NOW)).isEqualTo(
                new RetryBacklogQuery.Snapshot(3, 2, NOW.minusSeconds(90)));

        var cancelled = repository.findById(overdue.getId()).orElseThrow();
        cancelled.setStatus(ExecutionStatus.CANCELLED);
        cancelled.setNextAttemptAt(null);
        repository.saveAndFlush(cancelled);
        assertThat(query.read(NOW)).isEqualTo(
                new RetryBacklogQuery.Snapshot(2, 1, NOW));

        var released = repository.findById(exactlyDue.getId()).orElseThrow();
        released.setStatus(ExecutionStatus.QUEUED);
        released.setNextAttemptAt(null);
        repository.saveAndFlush(released);
        assertThat(query.read(NOW)).isEqualTo(
                new RetryBacklogQuery.Snapshot(1, 0, null));
    }

    private void setWaiting(long id, Instant dueAt) {
        var execution = repository.findById(id).orElseThrow();
        execution.setStatus(ExecutionStatus.RETRY_WAIT);
        execution.setNextAttemptAt(dueAt);
        repository.saveAndFlush(execution);
    }
}
