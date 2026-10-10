package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/** Periodically releases due retry attempts; can be disabled per deployment. */
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

    @Scheduled(
            fixedDelayString = "${flowforge.execution.retry.poll-interval-ms:5000}",
            initialDelayString = "${flowforge.execution.retry.poll-initial-delay-ms:5000}")
    public void releaseDueRetries() {
        try {
            int requeued = dueRetryService.requeueDueRetries();
            if (requeued > 0) {
                log.info("Requeued {} due workflow execution retries", requeued);
            }
        } catch (RuntimeException ex) {
            log.error("Failed to poll due execution retries; will retry next cycle", ex);
        }
    }
}
