package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/** Disabled by default: timeout recovery must be enabled deliberately. */
@Configuration
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "flowforge.execution.recovery", name = "enabled", havingValue = "true")
public class StaleRunningRecoveryScheduler {
    private static final Logger log = LoggerFactory.getLogger(StaleRunningRecoveryScheduler.class);
    private final StaleRunningRecoveryService recovery;

    @Scheduled(
            fixedDelayString = "${flowforge.execution.recovery.poll-interval-ms:60000}",
            initialDelayString = "${flowforge.execution.recovery.poll-initial-delay-ms:60000}")
    public void recover() {
        try {
            int count = recovery.recoverStaleNoopExecutions();
            if (count > 0) {
                log.warn("Recovered {} stale NOOP workflow execution attempts", count);
            }
        } catch (RuntimeException ex) {
            log.error("Stale NOOP recovery failed; will retry next cycle", ex);
        }
    }
}
