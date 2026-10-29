package com.flowforge.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DueRetrySchedulerMetricsTest {
    private final DueRetryService retries = mock(DueRetryService.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final DueRetryScheduler scheduler = new DueRetryScheduler(retries, meters);

    @AfterEach
    void closeMeters() {
        meters.close();
    }

    @Test
    void successfulPollsCountOnlyCommittedRequeues() {
        when(retries.requeueDueRetries()).thenReturn(3, 0, 2);

        scheduler.releaseDueRetries();
        scheduler.releaseDueRetries();
        scheduler.releaseDueRetries();

        verify(retries, times(3)).requeueDueRetries();
        assertThat(meters.get("flowforge.execution.retry.cycles")
                .tag("result", "succeeded").counter().count()).isEqualTo(3);
        assertThat(meters.get("flowforge.execution.retry.requeued")
                .counter().count()).isEqualTo(5);
        assertThat(meters.get("flowforge.execution.retry.cycle.duration")
                .tag("result", "succeeded").timer().count()).isEqualTo(3);
        assertThat(meters.find("flowforge.execution.retry.cycles")
                .tag("result", "failed").counter()).isNull();
    }

    @Test
    void emptyPollStillReportsHealthySchedulerCycle() {
        when(retries.requeueDueRetries()).thenReturn(0);

        scheduler.releaseDueRetries();

        assertThat(meters.get("flowforge.execution.retry.cycles")
                .tag("result", "succeeded").counter().count()).isEqualTo(1);
        assertThat(meters.find("flowforge.execution.retry.requeued").counter()).isNull();
    }

    @Test
    void databaseOrOutboxFailureDoesNotCountRolledBackRequeues() {
        when(retries.requeueDueRetries())
                .thenThrow(new IllegalStateException("database unavailable"))
                .thenReturn(2);

        // A failed poll is observable but must not stop future scheduled polls.
        scheduler.releaseDueRetries();
        assertThat(meters.get("flowforge.execution.retry.cycles")
                .tag("result", "failed").counter().count()).isEqualTo(1);
        assertThat(meters.get("flowforge.execution.retry.cycle.duration")
                .tag("result", "failed").timer().count()).isEqualTo(1);
        assertThat(meters.find("flowforge.execution.retry.requeued").counter()).isNull();

        scheduler.releaseDueRetries();
        assertThat(meters.get("flowforge.execution.retry.cycles")
                .tag("result", "succeeded").counter().count()).isEqualTo(1);
        assertThat(meters.get("flowforge.execution.retry.requeued")
                .counter().count()).isEqualTo(2);
        verify(retries, times(2)).requeueDueRetries();
    }
}
