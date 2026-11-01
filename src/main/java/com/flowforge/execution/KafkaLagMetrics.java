package com.flowforge.execution;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Broker calls run only on the scheduled sampler thread, never on a Prometheus
 * scrape. Failed polls preserve the last complete snapshot and its timestamp.
 */
@Slf4j
final class KafkaLagMetrics {
    private record Sample(long dispatchLag, long dltRetained, long lastSuccessEpochSeconds) {}

    private final KafkaLagQuery query;
    private final Clock clock;
    private final MeterRegistry meters;
    private final AtomicReference<Sample> sample =
            new AtomicReference<>(new Sample(0, 0, 0));

    KafkaLagMetrics(KafkaLagQuery query, Clock clock, MeterRegistry meters) {
        this.query = query;
        this.clock = clock;
        this.meters = meters;
        Gauge.builder("flowforge.kafka.dispatch.lag", sample,
                ref -> ref.get().dispatchLag()).register(meters);
        Gauge.builder("flowforge.kafka.dead_letter.retained", sample,
                ref -> ref.get().dltRetained()).register(meters);
        Gauge.builder("flowforge.kafka.offsets.last_success_timestamp", sample,
                ref -> ref.get().lastSuccessEpochSeconds())
                .baseUnit("seconds").register(meters);
        meters.counter("flowforge.kafka.offsets.polls", "result", "succeeded");
        meters.counter("flowforge.kafka.offsets.polls", "result", "failed");
    }

    @org.springframework.scheduling.annotation.Scheduled(
            fixedDelayString = "${flowforge.worker.consumer.lag.poll-interval-ms:30000}",
            initialDelayString = "${flowforge.worker.consumer.lag.poll-initial-delay-ms:30000}")
    public void refresh() {
        try {
            var offsets = query.read();
            sample.set(new Sample(offsets.dispatchLag(), offsets.dltRetained(),
                    clock.instant().getEpochSecond()));
            meters.counter("flowforge.kafka.offsets.polls", "result", "succeeded").increment();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            failed(ex);
        } catch (Exception ex) {
            failed(ex);
        }
    }

    private void failed(Exception ex) {
        meters.counter("flowforge.kafka.offsets.polls", "result", "failed").increment();
        log.error("Unable to sample Kafka offsets; retaining last successful snapshot", ex);
    }
}
