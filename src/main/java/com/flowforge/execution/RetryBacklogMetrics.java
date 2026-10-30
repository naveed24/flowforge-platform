package com.flowforge.execution;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Opt-in, bounded-frequency DB sampling. Prometheus scrapes only an in-memory
 * immutable snapshot and never executes a database query on the scrape thread.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "flowforge.execution.retry.backlog",
        name = "enabled", havingValue = "true")
public class RetryBacklogMetrics {
    private record Sample(long waiting, long due, long oldestDueAgeSeconds,
                          long lastSuccessEpochSeconds) {}

    private final RetryBacklogQuery query;
    private final Clock clock;
    private final MeterRegistry meters;
    private final AtomicReference<Sample> sample =
            new AtomicReference<>(new Sample(0, 0, 0, 0));

    public RetryBacklogMetrics(RetryBacklogQuery query, Clock clock, MeterRegistry meters) {
        this.query = query;
        this.clock = clock;
        this.meters = meters;
        Gauge.builder("flowforge.execution.retry.backlog", sample,
                        ref -> ref.get().waiting())
                .tag("state", "waiting").register(meters);
        Gauge.builder("flowforge.execution.retry.backlog", sample,
                        ref -> ref.get().due())
                .tag("state", "due").register(meters);
        Gauge.builder("flowforge.execution.retry.backlog.oldest_due_age", sample,
                        ref -> ref.get().oldestDueAgeSeconds())
                .baseUnit("seconds").register(meters);
        Gauge.builder("flowforge.execution.retry.backlog.last_success_timestamp", sample,
                        ref -> ref.get().lastSuccessEpochSeconds())
                .baseUnit("seconds").register(meters);
        // Publish both result series even before the first successful sample.
        meters.counter("flowforge.execution.retry.backlog.polls", "result", "succeeded");
        meters.counter("flowforge.execution.retry.backlog.polls", "result", "failed");
    }

    @Scheduled(
            fixedDelayString = "${flowforge.execution.retry.backlog.poll-interval-ms:30000}",
            initialDelayString = "${flowforge.execution.retry.backlog.poll-initial-delay-ms:30000}")
    public void refresh() {
        try {
            var now = clock.instant();
            var counts = query.read(now);
            long age = counts.oldestDueAt() == null ? 0
                    : Math.max(0, Duration.between(counts.oldestDueAt(), now).getSeconds());
            // Atomic swap avoids partial gauge updates during concurrent scrapes.
            sample.set(new Sample(counts.waiting(), counts.due(), age, now.getEpochSecond()));
            meters.counter("flowforge.execution.retry.backlog.polls",
                    "result", "succeeded").increment();
        } catch (RuntimeException ex) {
            // Retain last good sample and timestamp: stale data must be
            // detectable instead of reporting a misleading zero backlog.
            meters.counter("flowforge.execution.retry.backlog.polls",
                    "result", "failed").increment();
            log.error("Unable to refresh retry backlog metrics; retaining last good snapshot", ex);
        }
    }
}
