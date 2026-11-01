package com.flowforge.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class KafkaLagMetricsTest {
    private static final Instant NOW = Instant.parse("2026-11-01T02:30:00Z");
    private final KafkaLagQuery query = mock(KafkaLagQuery.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final KafkaLagMetrics metrics =
            new KafkaLagMetrics(query, Clock.fixed(NOW, ZoneOffset.UTC), meters);

    @AfterEach
    void close() {
        meters.close();
    }

    @Test
    void publishesCompleteSnapshotWithoutBrokerCallsOnPrometheusScrape() throws Exception {
        when(query.read()).thenReturn(new KafkaLagQuery.Snapshot(31, 8));
        metrics.refresh();

        assertThat(gauge("flowforge.kafka.dispatch.lag")).isEqualTo(31);
        assertThat(gauge("flowforge.kafka.dead_letter.retained")).isEqualTo(8);
        assertThat(gauge("flowforge.kafka.offsets.last_success_timestamp"))
                .isEqualTo(NOW.getEpochSecond());
        assertThat(polls("succeeded")).isEqualTo(1);
        assertThat(polls("failed")).isZero();
        gauge("flowforge.kafka.dispatch.lag");
        verify(query, times(1)).read();
        verifyNoMoreInteractions(query);
    }

    @Test
    void brokerFailurePreservesPreviousSnapshotAndRecoveryUpdatesIt() throws Exception {
        when(query.read())
                .thenReturn(new KafkaLagQuery.Snapshot(7, 3))
                .thenThrow(new IllegalStateException("Kafka unavailable"))
                .thenReturn(new KafkaLagQuery.Snapshot(0, 0));

        metrics.refresh();
        metrics.refresh();

        assertThat(gauge("flowforge.kafka.dispatch.lag")).isEqualTo(7);
        assertThat(gauge("flowforge.kafka.dead_letter.retained")).isEqualTo(3);
        assertThat(gauge("flowforge.kafka.offsets.last_success_timestamp"))
                .isEqualTo(NOW.getEpochSecond());
        assertThat(polls("succeeded")).isEqualTo(1);
        assertThat(polls("failed")).isEqualTo(1);

        metrics.refresh();
        assertThat(gauge("flowforge.kafka.dispatch.lag")).isZero();
        assertThat(gauge("flowforge.kafka.dead_letter.retained")).isZero();
        assertThat(polls("succeeded")).isEqualTo(2);
    }

    @Test
    void beforeFirstSuccessfulSampleZeroIsNotEvidenceOfHealthyBacklog() throws Exception {
        when(query.read()).thenThrow(new IllegalStateException("missing topic"));
        metrics.refresh();

        assertThat(gauge("flowforge.kafka.offsets.last_success_timestamp")).isZero();
        assertThat(polls("failed")).isEqualTo(1);
    }

    private double gauge(String name) {
        return meters.get(name).gauge().value();
    }

    private double polls(String result) {
        return meters.get("flowforge.kafka.offsets.polls")
                .tag("result", result).counter().count();
    }
}
