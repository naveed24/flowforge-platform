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
import org.springframework.test.context.ActiveProfiles;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Import(ExecutionDispatchOutboxIntegrationTest.TestClockConfig.class)
class ExecutionDispatchOutboxIntegrationTest {

    @Autowired private WorkflowService workflows;
    @Autowired private ExecutionService executions;
    @Autowired private ExecutionSubmissionService submissions;
    @Autowired private ExecutionClaimService claims;
    @Autowired private ExecutionAttemptCompletionService completion;
    @Autowired private DueRetryService retries;
    @Autowired private ExecutionDispatchOutboxRepository outbox;
    @Autowired private MutableClock clock;

    @BeforeEach void resetClock() {
        clock.reset();
    }

    @Test
    void initialEnqueueAndIdempotentReplayProduceExactlyOneDispatchIntent() {
        long workflowId = activeWorkflow();
        var first = submissions.submit(workflowId, "request-" + UUID.randomUUID());
        var replay = submissions.submit(workflowId, first.execution().getIdempotencyKey());

        assertThat(first.replayed()).isFalse();
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.execution().getId()).isEqualTo(first.execution().getId());
        var events = outbox.findByExecution_IdOrderByAttemptNumberAsc(first.execution().getId());
        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getAttemptNumber()).isEqualTo(1);
        assertThat(events.getFirst().getPublishedAt()).isNull();
        assertThat(events.getFirst().getCreatedAt()).isEqualTo(clock.instant());

        var keyless = executions.enqueue(workflowId);
        assertThat(outbox.findByExecution_IdOrderByAttemptNumberAsc(keyless.getId()))
                .extracting(ExecutionDispatchOutbox::getAttemptNumber)
                .containsExactly(1);
    }

    @Test
    void dueRetriesEmitOneNewIntentPerAttemptAndNeverAfterExhaustion() {
        long id = executions.enqueue(activeWorkflow()).getId();
        for (int attempt = 1; attempt <= RetryBackoffPolicy.DEFAULT_MAX_ATTEMPTS; attempt++) {
            assertThat(claims.claimQueuedById(id).orElseThrow().getAttemptCount()).isEqualTo(attempt);
            assertThat(completion.fail(id, attempt)).isTrue();
            if (attempt < RetryBackoffPolicy.DEFAULT_MAX_ATTEMPTS) {
                assertThat(retries.requeueDueRetries()).isZero();
                clock.advance(RetryBackoffPolicy.delayAfterFailure(attempt));
                assertThat(retries.requeueDueRetries()).isEqualTo(1);
                assertThat(retries.requeueDueRetries()).isZero();
            }
        }
        assertThat(executions.get(id).getStatus()).isEqualTo(ExecutionStatus.FAILED);
        clock.advance(Duration.ofHours(1));
        assertThat(retries.requeueDueRetries()).isZero();
        assertThat(outbox.findByExecution_IdOrderByAttemptNumberAsc(id))
                .extracting(ExecutionDispatchOutbox::getAttemptNumber)
                .containsExactly(1, 2, 3);
    }

    @Test
    void cancellationPreventsRetryDispatchIntent() {
        long id = executions.enqueue(activeWorkflow()).getId();
        assertThat(claims.claimQueuedById(id)).isPresent();
        assertThat(completion.fail(id, 1)).isTrue();
        var waiting = executions.get(id);
        executions.cancel(id, waiting.getVersion());
        clock.advance(Duration.ofHours(1));
        assertThat(retries.requeueDueRetries()).isZero();
        assertThat(outbox.findByExecution_IdOrderByAttemptNumberAsc(id))
                .extracting(ExecutionDispatchOutbox::getAttemptNumber)
                .containsExactly(1);
    }

    private long activeWorkflow() {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "outbox-" + UUID.randomUUID(), "Transactional dispatch intent test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));
        return workflows.activate(created.id(), created.version()).id();
    }

    @TestConfiguration
    static class TestClockConfig {
        @Bean @Primary
        MutableClock outboxTestClock() { return new MutableClock(); }
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
