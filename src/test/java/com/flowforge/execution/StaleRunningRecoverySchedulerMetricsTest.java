package com.flowforge.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class StaleRunningRecoverySchedulerMetricsTest {
    private final StaleRunningRecoveryService recovery = mock(StaleRunningRecoveryService.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final StaleRunningRecoveryScheduler scheduler =
            new StaleRunningRecoveryScheduler(recovery, meters);

    @Test
    void successfulCyclesCountRecoveredAttemptsAndDuration() {
        when(recovery.recoverStaleNoopExecutions()).thenReturn(3, 0, 2);

        scheduler.recover();
        scheduler.recover();
        scheduler.recover();

        verify(recovery, times(3)).recoverStaleNoopExecutions();
        assertThat(meters.get("flowforge.execution.recovery.cycles.total")
                .tag("result", "succeeded").counter().count()).isEqualTo(3);
        assertThat(meters.get("flowforge.execution.recovery.executions.total")
                .counter().count()).isEqualTo(5);
        assertThat(meters.get("flowforge.execution.recovery.cycle.duration")
                .tag("result", "succeeded").timer().count()).isEqualTo(3);
    }

    @Test
    void failedCycleIsVisibleAndDoesNotReportRecoveredWork() {
        when(recovery.recoverStaleNoopExecutions())
                .thenThrow(new IllegalStateException("Redis unavailable"));

        // Scheduler must swallow the error so the next scheduled poll can run.
        scheduler.recover();

        assertThat(meters.get("flowforge.execution.recovery.cycles.total")
                .tag("result", "failed").counter().count()).isEqualTo(1);
        assertThat(meters.get("flowforge.execution.recovery.cycle.duration")
                .tag("result", "failed").timer().count()).isEqualTo(1);
        assertThat(meters.find("flowforge.execution.recovery.executions.total")
                .counter()).isNull();
    }
}
