package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class NoopWorkflowDispatchServiceIntegrationTest {

    @Autowired private WorkflowService workflows;
    @Autowired private ExecutionService executions;
    @Autowired private WorkflowExecutionRepository executionRepository;
    @Autowired private ExecutionDispatchOutboxRepository outbox;
    @Autowired private NoopWorkflowDispatchService worker;

    @BeforeEach
    void clear() {
        outbox.deleteAllInBatch();
        executionRepository.deleteAllInBatch();
    }

    @Test
    void completesNoopWorkflowOnceAndIgnoresDuplicateKafkaDelivery() {
        long id = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        var event = eventFor(id);

        assertThat(worker.process(event)).isEqualTo(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        var completed = executions.get(id);
        assertThat(completed.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(completed.getAttemptCount()).isEqualTo(1);
        assertThat(completed.getFinishedAt()).isNotNull();

        assertThat(worker.process(event))
                .isEqualTo(NoopWorkflowDispatchService.Outcome.DUPLICATE_OR_STALE);
        assertThat(executions.get(id).getAttemptCount()).isEqualTo(1);
    }

    @Test
    void unsupportedTaskTypesFailThroughExistingRetryStateMachine() {
        long id = executions.enqueue(activeWorkflow(TaskType.HTTP)).getId();
        var event = eventFor(id);

        assertThat(worker.process(event))
                .isEqualTo(NoopWorkflowDispatchService.Outcome.UNSUPPORTED_RETRY);
        var failedAttempt = executions.get(id);
        assertThat(failedAttempt.getStatus()).isEqualTo(ExecutionStatus.RETRY_WAIT);
        assertThat(failedAttempt.getAttemptCount()).isEqualTo(1);
        assertThat(failedAttempt.getNextAttemptAt()).isNotNull();
        assertThat(worker.process(event))
                .isEqualTo(NoopWorkflowDispatchService.Outcome.DUPLICATE_OR_STALE);
    }

    @Test
    void rejectsMismatchedOutboxIdentityWithoutClaimingExecution() {
        long first = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        long second = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        var other = eventFor(second);

        assertThatThrownBy(() -> worker.process(
                new ExecutionDispatchMessage(first, 1, other.outboxId())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> worker.process(
                new ExecutionDispatchMessage(first, 2, eventFor(first).outboxId())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(executions.get(first).getStatus()).isEqualTo(ExecutionStatus.QUEUED);
        assertThat(executions.get(second).getStatus()).isEqualTo(ExecutionStatus.QUEUED);
    }

    @Test
    void listenerAcknowledgesOnlyAfterSuccessfulDatabaseProcessing() {
        long id = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        var event = eventFor(id);
        var listener = new ExecutionDispatchConsumer(worker);
        var ack = mock(Acknowledgment.class);

        listener.onMessage(record(Long.toString(id), event), ack);
        verify(ack).acknowledge();
        assertThat(executions.get(id).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);

        var badAck = mock(Acknowledgment.class);
        assertThatThrownBy(() -> listener.onMessage(record("different-key", event), badAck))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(badAck);
    }

    private ConsumerRecord<String, ExecutionDispatchMessage> record(
            String key, ExecutionDispatchMessage event) {
        return new ConsumerRecord<>("flowforge.execution.dispatch.v1", 0, 0L, key, event);
    }

    private ExecutionDispatchMessage eventFor(long id) {
        var row = outbox.findByExecution_IdOrderByAttemptNumberAsc(id).getFirst();
        return new ExecutionDispatchMessage(id, row.getAttemptNumber(), row.getId());
    }

    private long activeWorkflow(TaskType taskType) {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "noop-worker-" + UUID.randomUUID(), "Kafka worker integration test",
                List.of(new WorkflowDtos.TaskRequest("run", taskType, Set.of()))));
        return workflows.activate(created.id(), created.version()).id();
    }
}
