package com.flowforge.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class ExecutionDispatchConsumerValidationTest {
    private final WorkerDispatchLeaseCoordinator worker = mock(WorkerDispatchLeaseCoordinator.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final ExecutionDispatchConsumer consumer = new ExecutionDispatchConsumer(worker, meters);

    @Test
    void rejectsNullRecordWithoutAcknowledgment() {
        assertThatThrownBy(() -> consumer.onMessage(null, ack))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(worker, ack);
        assertThat(count("invalid")).isEqualTo(1);
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
        assertThat(count("invalid")).isEqualTo(8);
        assertThat(meters.get("flowforge.worker.dispatch.duration")
                .tag("result", "invalid").timer().count()).isEqualTo(8);
    }

    @Test
    void successfulDispatchAcknowledgesOnlyAfterWorkerReturns() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        consumer.onMessage(record("1", event), ack);
        var ordered = inOrder(worker, ack);
        ordered.verify(worker).process(event);
        ordered.verify(ack).acknowledge();
        assertThat(count("succeeded")).isEqualTo(1);
        assertThat(meters.get("flowforge.worker.dispatch.duration")
                .tag("result", "succeeded").timer().count()).isEqualTo(1);
    }

    @Test
    void duplicateDeliveryHasSeparateBoundedOutcome() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.DUPLICATE_OR_STALE);
        consumer.onMessage(record("1", event), ack);
        assertThat(count("duplicate_or_stale")).isEqualTo(1);
    }

    @Test
    void unsupportedTasksAreCountedAfterAcknowledgment() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.UNSUPPORTED_RETRY);
        consumer.onMessage(record("1", event), ack);
        assertThat(count("unsupported_retry")).isEqualTo(1);
    }

    @Test
    void workerFailureNeverAcknowledgesAndIsCounted() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenThrow(new IllegalStateException("database unavailable"));
        assertThatThrownBy(() -> consumer.onMessage(record("1", event), ack))
                .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(ack);
        assertThat(count("failed")).isEqualTo(1);
    }

    @Test
    void acknowledgmentFailureCannotBeCountedAsSuccess() {
        var event = new ExecutionDispatchMessage(1, 1, 1);
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        doThrow(new IllegalStateException("offset commit failed")).when(ack).acknowledge();
        assertThatThrownBy(() -> consumer.onMessage(record("1", event), ack))
                .isInstanceOf(IllegalStateException.class);
        assertThat(count("failed")).isEqualTo(1);
        assertThat(meters.find("flowforge.worker.dispatch")
                .tag("result", "succeeded").counter()).isNull();
    }

    private double count(String result) {
        return meters.get("flowforge.worker.dispatch").tag("result", result).counter().count();
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
