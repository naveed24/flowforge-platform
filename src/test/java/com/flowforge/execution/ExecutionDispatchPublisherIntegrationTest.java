package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@Import(ExecutionDispatchPublisherIntegrationTest.TestClockConfig.class)
class ExecutionDispatchPublisherIntegrationTest {

    @Autowired private WorkflowService workflows;
    @Autowired private ExecutionService executions;
    @Autowired private ExecutionClaimService executionClaims;
    @Autowired private ExecutionAttemptCompletionService completion;
    @Autowired private DueRetryService retries;
    @Autowired private ExecutionDispatchClaimService dispatchClaims;
    @Autowired private ExecutionDispatchPublisher publisher;
    @Autowired private ExecutionDispatchOutboxRepository outbox;
    @Autowired private MutableClock clock;

    @MockitoBean private KafkaTemplate<String, ExecutionDispatchMessage> kafka;

    @BeforeEach
    void resetFixtures() {
        outbox.deleteAll();
        clock.reset();
        reset(kafka);
    }

    @Test
    void publishesOnlyAfterKafkaAckAndNeverReplaysAcknowledgedRow() {
        long id = executions.enqueue(activeWorkflow()).getId();
        when(kafka.send(eq("flowforge.execution.dispatch.v1"), eq(Long.toString(id)),
                any(ExecutionDispatchMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThat(publisher.publishPendingBatch()).isEqualTo(1);
        assertThat(publisher.publishPendingBatch()).isZero();
        var row = outbox.findByExecution_IdOrderByAttemptNumberAsc(id).getFirst();
        assertThat(row.getPublishedAt()).isEqualTo(clock.instant());
        assertThat(row.getClaimToken()).isNull();
        assertThat(row.getClaimedUntil()).isNull();
        assertThat(row.getPublishAttempts()).isEqualTo(1);
        verify(kafka, times(1)).send(eq("flowforge.execution.dispatch.v1"),
                eq(Long.toString(id)), eq(new ExecutionDispatchMessage(id, 1, row.getId())));
    }

    @Test
    void failedSendIsNotAcknowledgedAndRetriesWithBackoff() {
        long id = executions.enqueue(activeWorkflow()).getId();
        when(kafka.send(eq("flowforge.execution.dispatch.v1"), anyString(),
                any(ExecutionDispatchMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")),
                        CompletableFuture.completedFuture(null));

        assertThat(publisher.publishPendingBatch()).isZero();
        var failed = outbox.findByExecution_IdOrderByAttemptNumberAsc(id).getFirst();
        assertThat(failed.getPublishedAt()).isNull();
        assertThat(failed.getNextPublishAt()).isEqualTo(clock.instant().plusSeconds(5));
        assertThat(failed.getLastError()).isEqualTo("ExecutionException");
        assertThat(publisher.publishPendingBatch()).isZero();

        clock.advance(Duration.ofSeconds(5));
        assertThat(publisher.publishPendingBatch()).isEqualTo(1);
        var delivered = outbox.findByExecution_IdOrderByAttemptNumberAsc(id).getFirst();
        assertThat(delivered.getPublishedAt()).isNotNull();
        assertThat(delivered.getPublishAttempts()).isEqualTo(2);
        verify(kafka, times(2)).send(eq("flowforge.execution.dispatch.v1"),
                eq(Long.toString(id)), any(ExecutionDispatchMessage.class));
    }

    @Test
    void expiredLeaseIsReclaimableAndStaleTokenCannotAcknowledge() {
        long id = executions.enqueue(activeWorkflow()).getId();
        var first = dispatchClaims.claimNext().orElseThrow();
        assertThat(dispatchClaims.claimNext()).isEmpty();

        clock.advance(Duration.ofSeconds(61));
        var replacement = dispatchClaims.claimNext().orElseThrow();
        assertThat(replacement.outboxId()).isEqualTo(first.outboxId());
        assertThat(replacement.token()).isNotEqualTo(first.token());
        assertThat(dispatchClaims.markPublished(first)).isFalse();
        assertThat(dispatchClaims.releaseForRetry(first, "StaleSender")).isFalse();
        assertThat(dispatchClaims.markPublished(replacement)).isTrue();
        assertThat(outbox.findByExecution_IdOrderByAttemptNumberAsc(id).getFirst().getPublishAttempts())
                .isEqualTo(2);
    }

    @Test
    void oldAttemptMessageCannotClaimRequeuedExecution() {
        long id = executions.enqueue(activeWorkflow()).getId();
        assertThat(executionClaims.claimQueuedById(id, 1)).isPresent();
        assertThat(completion.fail(id, 1)).isTrue();
        clock.advance(RetryBackoffPolicy.delayAfterFailure(1));
        assertThat(retries.requeueDueRetries()).isEqualTo(1);

        assertThat(executionClaims.claimQueuedById(id, 1)).isEmpty();
        assertThat(executionClaims.claimQueuedById(id, 2)).isPresent();
        assertThat(executionClaims.claimQueuedById(id, 2)).isEmpty();
    }

    private long activeWorkflow() {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "publisher-" + UUID.randomUUID(), "Kafka outbox publisher integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));
        return workflows.activate(created.id(), created.version()).id();
    }

    @TestConfiguration
    static class TestClockConfig {
        @Bean @Primary
        MutableClock publisherTestClock() { return new MutableClock(); }
    }

    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2020-01-01T00:00:00Z");
        void reset() { now = Instant.parse("2020-01-01T00:00:00Z"); }
        void advance(Duration duration) { now = now.plus(duration); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(now, zone); }
        @Override public Instant instant() { return now; }
    }
}
