package com.flowforge.execution;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Periodically releases due retry attempts. Metrics count committed requeues,
 * not merely scanned candidates or retries that have started executing.
 */
@Configuration
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "flowforge.execution.retry",
        name = "polling-enabled",
        havingValue = "true",
        matchIfMissing = true)
public class DueRetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(DueRetryScheduler.class);
    private final DueRetryService dueRetryService;
    private final MeterRegistry meters;

    @Scheduled(
            fixedDelayString = "${flowforge.execution.retry.poll-interval-ms:5000}",
            initialDelayString = "${flowforge.execution.retry.poll-initial-delay-ms:5000}")
    public void releaseDueRetries() {
        Timer.Sample sample = Timer.start(meters);
        String result = "failed";
        try {
            // requeueDueRetries is transactional; a normal return means that
            // the conditional state transitions and outbox writes committed.
            int requeued = dueRetryService.requeueDueRetries();
            if (requeued > 0) {
                meters.counter("flowforge.execution.retry.requeued").increment(requeued);
                log.info("Requeued {} due workflow execution retries", requeued);
            }
            result = "succeeded";
        } catch (RuntimeException ex) {
            log.error("Failed to poll due execution retries; will retry next cycle", ex);
        } finally {
            // Fixed result values only: never label with execution or workflow IDs.
            meters.counter("flowforge.execution.retry.cycles", "result", result).increment();
            sample.stop(meters.timer("flowforge.execution.retry.cycle.duration", "result", result));
        }
    }
}
