package com.flowforge.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RetryBacklogMetricsTest {
    private static final Instant NOW = Instant.parse("2026-10-30T08:00:00Z");
    private final RetryBacklogQuery query = mock(RetryBacklogQuery.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final RetryBacklogMetrics metrics = new RetryBacklogMetrics(
            query, Clock.fixed(NOW, ZoneOffset.UTC), meters);

    @AfterEach
    void close() {
        meters.close();
    }

    @Test
    void samplesFullBacklogAndOldestOverdueAgeWithoutQueryingDuringScrape() {
        when(query.read(NOW)).thenReturn(
                new RetryBacklogQuery.Snapshot(9, 3, NOW.minusSeconds(125)));

        metrics.refresh();

        assertThat(gauge("flowforge.execution.retry.backlog", "state", "waiting"))
                .isEqualTo(9);
        assertThat(gauge("flowforge.execution.retry.backlog", "state", "due"))
                .isEqualTo(3);
        assertThat(gauge("flowforge.execution.retry.backlog.oldest_due_age"))
                .isEqualTo(125);
        assertThat(gauge("flowforge.execution.retry.backlog.last_success_timestamp"))
                .isEqualTo(NOW.getEpochSecond());
        assertThat(polls("succeeded")).isEqualTo(1);
        assertThat(polls("failed")).isZero();
        verify(query, times(1)).read(NOW);

        // Reading metrics does not hit PostgreSQL.
        gauge("flowforge.execution.retry.backlog", "state", "due");
        verifyNoMoreInteractions(query);
    }

    @Test
    void emptyBacklogAndFutureTimestampsProduceZeroAge() {
        when(query.read(NOW)).thenReturn(
                new RetryBacklogQuery.Snapshot(0, 0, null),
                new RetryBacklogQuery.Snapshot(1, 1, NOW.plusSeconds(2)));

        metrics.refresh();
        assertThat(gauge("flowforge.execution.retry.backlog.oldest_due_age")).isZero();
        assertThat(gauge("flowforge.execution.retry.backlog", "state", "due")).isZero();

        metrics.refresh();
        assertThat(gauge("flowforge.execution.retry.backlog.oldest_due_age")).isZero();
        assertThat(polls("succeeded")).isEqualTo(2);
    }

    @Test
    void failedRefreshKeepsPreviousSnapshotAndExposesStaleness() {
        when(query.read(NOW))
                .thenReturn(new RetryBacklogQuery.Snapshot(5, 2, NOW.minusSeconds(40)))
                .thenThrow(new IllegalStateException("database unavailable"))
                .thenReturn(new RetryBacklogQuery.Snapshot(1, 0, null));

        metrics.refresh();
        metrics.refresh();

        assertThat(gauge("flowforge.execution.retry.backlog", "state", "waiting"))
                .isEqualTo(5);
        assertThat(gauge("flowforge.execution.retry.backlog", "state", "due"))
                .isEqualTo(2);
        assertThat(gauge("flowforge.execution.retry.backlog.oldest_due_age"))
                .isEqualTo(40);
        assertThat(gauge("flowforge.execution.retry.backlog.last_success_timestamp"))
                .isEqualTo(NOW.getEpochSecond());
        assertThat(polls("succeeded")).isEqualTo(1);
        assertThat(polls("failed")).isEqualTo(1);

        metrics.refresh();
        assertThat(gauge("flowforge.execution.retry.backlog", "state", "waiting"))
                .isEqualTo(1);
        assertThat(gauge("flowforge.execution.retry.backlog", "state", "due"))
                .isZero();
        assertThat(polls("succeeded")).isEqualTo(2);
    }

    private double gauge(String name) {
        return meters.get(name).gauge().value();
    }

    private double gauge(String name, String tag, String value) {
        return meters.get(name).tag(tag, value).gauge().value();
    }

    private double polls(String result) {
        return meters.get("flowforge.execution.retry.backlog.polls")
                .tag("result", result).counter().count();
    }
}
