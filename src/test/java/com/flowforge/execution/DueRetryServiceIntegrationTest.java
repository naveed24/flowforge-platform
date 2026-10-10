package com.flowforge.execution;

import com.flowforge.common.ConflictException;
import com.flowforge.workflow.*;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
@Import(DueRetryServiceIntegrationTest.ClockConfig.class)
class DueRetryServiceIntegrationTest {

    @Autowired private WorkflowService workflowService;
    @Autowired private ExecutionService executionService;
    @Autowired private DueRetryService dueRetryService;
    @Autowired private MutableClock clock;

    @BeforeEach
    void resetClock() {
        clock.reset();
    }

    @Test
    void requeuesOnlyDueRetriesOnceWithoutConsumingAnAttempt() {
        var workflow = activeWorkflow("due-retry-release");
        var queued = executionService.enqueue(workflow.id());
        executionService.start(queued.getId());
        var waiting = executionService.fail(queued.getId());
        long previousVersion = waiting.getVersion();

        assertThat(dueRetryService.requeueDueRetries()).isZero();
        clock.advance(Duration.ofSeconds(29));
        assertThat(dueRetryService.requeueDueRetries()).isZero();

        clock.advance(Duration.ofSeconds(1));
        assertThat(dueRetryService.requeueDueRetries()).isEqualTo(1);
        var released = executionService.get(queued.getId());
        assertThat(released.getStatus()).isEqualTo(ExecutionStatus.QUEUED);
        assertThat(released.getNextAttemptAt()).isNull();
        assertThat(released.getAttemptCount()).isEqualTo(1);
        assertThat(released.getVersion()).isGreaterThan(previousVersion);
        assertThat(dueRetryService.requeueDueRetries()).isZero();

        assertThatThrownBy(() -> executionService.cancel(queued.getId(), previousVersion))
                .isInstanceOf(ConflictException.class);

        var running = executionService.start(queued.getId());
        assertThat(running.getAttemptCount()).isEqualTo(2);
        var waitingAgain = executionService.fail(queued.getId());
        assertThat(waitingAgain.getNextAttemptAt()).isEqualTo(clock.instant().plusSeconds(60));

        clock.advance(Duration.ofSeconds(59));
        assertThat(dueRetryService.requeueDueRetries()).isZero();
        clock.advance(Duration.ofSeconds(1));
        assertThat(dueRetryService.requeueDueRetries()).isEqualTo(1);
        assertThat(executionService.get(queued.getId()).getAttemptCount()).isEqualTo(2);
    }

    @Test
    void cancelledRetryIsNeverRequeued() {
        var workflow = activeWorkflow("due-retry-cancelled");
        var queued = executionService.enqueue(workflow.id());
        executionService.start(queued.getId());
        var waiting = executionService.fail(queued.getId());
        executionService.cancel(waiting.getId(), waiting.getVersion());

        clock.advance(Duration.ofHours(1));
        assertThat(dueRetryService.requeueDueRetries()).isZero();
        var cancelled = executionService.get(queued.getId());
        assertThat(cancelled.getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
        assertThat(cancelled.getNextAttemptAt()).isNull();
    }

    private WorkflowDtos.WorkflowResponse activeWorkflow(String name) {
        var created = workflowService.create(new WorkflowDtos.CreateWorkflowRequest(
                name,
                "Automatic retry requeue integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))
        ));
        return workflowService.activate(created.id(), created.version());
    }

    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        MutableClock retryPollingTestClock() {
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
