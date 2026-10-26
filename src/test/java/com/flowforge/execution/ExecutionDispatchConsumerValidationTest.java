package com.flowforge.execution;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class ExecutionDispatchConsumerValidationTest {
    private final WorkerDispatchLeaseCoordinator worker = mock(WorkerDispatchLeaseCoordinator.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final ExecutionDispatchConsumer consumer = new ExecutionDispatchConsumer(worker);

    @Test
    void rejectsNullRecordWithoutAcknowledgment() {
        assertThatThrownBy(() -> consumer.onMessage(null, ack))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(worker, ack);
    }

    @Test
    void rejectsNullPayloadWithoutAcknowledgment() {
        reject("1", null);
    }

    @Test
    void rejectsInvalidIdentifiersAndAttemptsBeforeWorkerInvocation() {
        reject("0", new ExecutionDispatchMessage(0, 1, 1));
        reject("-1", new ExecutionDispatchMessage(-1, 1, 1));
        reject("1", new ExecutionDispatchMessage(1, 0, 1));
        reject("1", new ExecutionDispatchMessage(1, -1, 1));
        reject("1", new ExecutionDispatchMessage(1, 1, 0));
        reject("1", new ExecutionDispatchMessage(1, 1, -1));
        reject("other", new ExecutionDispatchMessage(1, 1, 1));
        reject(null, new ExecutionDispatchMessage(1, 1, 1));
    }

    @Test
    void successfulDispatchAcknowledgesOnlyAfterWorkerReturns() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        consumer.onMessage(record("1", event), ack);
        var ordered = inOrder(worker, ack);
        ordered.verify(worker).process(event);
        ordered.verify(ack).acknowledge();
    }

    @Test
    void workerFailureNeverAcknowledges() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenThrow(new IllegalStateException("database unavailable"));
        assertThatThrownBy(() -> consumer.onMessage(record("1", event), ack))
                .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(ack);
    }

    private void reject(String key, ExecutionDispatchMessage event) {
        assertThatThrownBy(() -> consumer.onMessage(record(key, event), ack))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(worker, ack);
    }

    private ConsumerRecord<String, ExecutionDispatchMessage> record(
            String key, ExecutionDispatchMessage event) {
        return new ConsumerRecord<>("flowforge.execution.dispatch.v1", 0, 0L, key, event);
    }
}
