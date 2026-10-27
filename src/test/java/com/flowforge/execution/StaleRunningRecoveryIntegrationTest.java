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
@Import(StaleRunningRecoveryIntegrationTest.ClockConfig.class)
class StaleRunningRecoveryIntegrationTest {
    @Autowired private WorkflowService workflows;
    @Autowired private ExecutionService executions;
    @Autowired private WorkflowExecutionRepository repository;
    @Autowired private ExecutionDispatchOutboxRepository outbox;
    @Autowired private DueRetryService dueRetries;
    @Autowired private StaleRunningRecoveryService recovery;
    @Autowired private MutableClock clock;

    @BeforeEach
    void reset() {
        outbox.deleteAllInBatch();
        repository.deleteAllInBatch();
        clock.reset();
    }

    @Test
    void timedOutNoopAttemptReentersDurableRetryAndCreatesNextOutboxOnDueRequeue() {
        long id = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        var running = executions.start(id);
        long oldVersion = running.getVersion();

        clock.advance(Duration.ofMinutes(14).plusSeconds(59));
        assertThat(recovery.recoverStaleNoopExecutions()).isZero();
        assertThat(executions.get(id).getStatus()).isEqualTo(ExecutionStatus.RUNNING);

        clock.advance(Duration.ofSeconds(1));
        assertThat(recovery.recoverStaleNoopExecutions()).isEqualTo(1);
        var waiting = executions.get(id);
        assertThat(waiting.getStatus()).isEqualTo(ExecutionStatus.RETRY_WAIT);
        assertThat(waiting.getAttemptCount()).isEqualTo(1);
        assertThat(waiting.getNextAttemptAt()).isEqualTo(clock.instant().plusSeconds(30));
        assertThat(waiting.getVersion()).isGreaterThan(oldVersion);
        assertThat(recovery.recoverStaleNoopExecutions()).isZero();
        assertThat(outbox.findByExecution_IdOrderByAttemptNumberAsc(id)).hasSize(1);

        clock.advance(Duration.ofSeconds(30));
        assertThat(dueRetries.requeueDueRetries()).isEqualTo(1);
        assertThat(executions.get(id).getStatus()).isEqualTo(ExecutionStatus.QUEUED);
        assertThat(outbox.findByExecution_IdOrderByAttemptNumberAsc(id))
                .extracting(ExecutionDispatchOutbox::getAttemptNumber).containsExactly(1, 2);
    }

    @Test
    void exhaustedStaleNoopAttemptBecomesTerminalFailure() {
        long id = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        var execution = executions.get(id);
        execution.setMaxAttempts(1);
        repository.saveAndFlush(execution);
        executions.start(id);

        clock.advance(Duration.ofMinutes(15));
        assertThat(recovery.recoverStaleNoopExecutions()).isEqualTo(1);
        var failed = executions.get(id);
        assertThat(failed.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(failed.getAttemptCount()).isEqualTo(1);
        assertThat(failed.getNextAttemptAt()).isNull();
        assertThat(failed.getFinishedAt()).isEqualTo(clock.instant());
        assertThat(recovery.recoverStaleNoopExecutions()).isZero();
        assertThat(dueRetries.requeueDueRetries()).isZero();
    }

    @Test
    void neverRecoversNonNoopOrCancelledExecutions() {
        long http = executions.enqueue(activeWorkflow(TaskType.HTTP)).getId();
        executions.start(http);
        long cancelled = executions.enqueue(activeWorkflow(TaskType.NOOP)).getId();
        var running = executions.start(cancelled);
        executions.cancel(cancelled, running.getVersion());

        clock.advance(Duration.ofHours(2));
        assertThat(recovery.recoverStaleNoopExecutions()).isZero();
        assertThat(executions.get(http).getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(executions.get(cancelled).getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
    }

    private long activeWorkflow(TaskType type) {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "recovery-" + UUID.randomUUID(), "Stale attempt recovery test",
                List.of(new WorkflowDtos.TaskRequest("run", type, Set.of()))));
        return workflows.activate(created.id(), created.version()).id();
    }

    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        MutableClock recoveryTestClock() {
            return new MutableClock();
        }
    }

    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2020-01-01T00:00:00Z");

        void reset() {
            now = Instant.parse("2020-01-01T00:00:00Z");
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(now, zone);
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
