package com.flowforge.execution;

import com.flowforge.common.BadRequestException;
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
@Import(ExecutionRetryIntegrationTest.ClockConfig.class)
class ExecutionRetryIntegrationTest {

    @Autowired private WorkflowService workflowService;
    @Autowired private ExecutionService executionService;
    @Autowired private MutableClock clock;

    @BeforeEach
    void resetTime() {
        clock.reset();
    }

    @Test
    void retriesAfterScheduledDelaysAndFailsPermanentlyAfterThreeAttempts() {
        var workflow = activeWorkflow("retry-exhaustion");
        var queued = executionService.enqueue(workflow.id());

        assertThat(queued.getAttemptCount()).isZero();
        assertThat(queued.getMaxAttempts()).isEqualTo(3);
        assertThat(queued.getNextAttemptAt()).isNull();

        var first = executionService.start(queued.getId());
        assertThat(first.getAttemptCount()).isEqualTo(1);
        var waitOne = executionService.fail(first.getId());
        assertThat(waitOne.getStatus()).isEqualTo(ExecutionStatus.RETRY_WAIT);
        assertThat(waitOne.getNextAttemptAt()).isEqualTo(clock.instant().plusSeconds(30));
        assertThat(waitOne.getFinishedAt()).isNull();

        assertThatThrownBy(() -> executionService.start(first.getId()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("not due yet");
        assertThat(executionService.get(first.getId()).getAttemptCount()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(30));
        var second = executionService.start(first.getId());
        assertThat(second.getAttemptCount()).isEqualTo(2);
        assertThat(second.getNextAttemptAt()).isNull();
        var waitTwo = executionService.fail(second.getId());
        assertThat(waitTwo.getNextAttemptAt()).isEqualTo(clock.instant().plusSeconds(60));

        clock.advance(Duration.ofSeconds(60));
        var third = executionService.start(first.getId());
        assertThat(third.getAttemptCount()).isEqualTo(3);
        var failed = executionService.fail(third.getId());
        assertThat(failed.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(failed.getNextAttemptAt()).isNull();
        assertThat(failed.getFinishedAt()).isEqualTo(clock.instant());

        assertThatThrownBy(() -> executionService.start(failed.getId()))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void cancellingWaitingExecutionClearsTheRetrySchedule() {
        var workflow = activeWorkflow("retry-cancel");
        var queued = executionService.enqueue(workflow.id());
        executionService.start(queued.getId());
        var waiting = executionService.fail(queued.getId());

        var cancelled = executionService.cancel(waiting.getId(), waiting.getVersion());
        assertThat(cancelled.getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
        assertThat(cancelled.getNextAttemptAt()).isNull();
        assertThat(cancelled.getFinishedAt()).isEqualTo(clock.instant());

        clock.advance(Duration.ofHours(1));
        assertThatThrownBy(() -> executionService.start(cancelled.getId()))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void successfulSecondAttemptTerminatesWithoutFurtherRetries() {
        var workflow = activeWorkflow("retry-then-success");
        var queued = executionService.enqueue(workflow.id());
        executionService.start(queued.getId());
        executionService.fail(queued.getId());
        clock.advance(Duration.ofSeconds(30));
        executionService.start(queued.getId());

        var succeeded = executionService.succeed(queued.getId());
        assertThat(succeeded.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(succeeded.getAttemptCount()).isEqualTo(2);
        assertThat(succeeded.getNextAttemptAt()).isNull();
        assertThat(succeeded.getFinishedAt()).isEqualTo(clock.instant());
    }

    private WorkflowDtos.WorkflowResponse activeWorkflow(String name) {
        var created = workflowService.create(new WorkflowDtos.CreateWorkflowRequest(
                name,
                "Retry integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))
        ));
        return workflowService.activate(created.id(), created.version());
    }

    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        MutableClock mutableExecutionClock() {
            return new MutableClock();
        }
    }

    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-10-10T00:00:00Z");

        void reset() {
            now = Instant.parse("2026-10-10T00:00:00Z");
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
