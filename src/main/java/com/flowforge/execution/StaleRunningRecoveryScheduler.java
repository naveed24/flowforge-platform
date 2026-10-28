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

/** Disabled by default: timeout recovery must be enabled deliberately. */
@Configuration
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "flowforge.execution.recovery", name = "enabled", havingValue = "true")
public class StaleRunningRecoveryScheduler {
    private static final Logger log = LoggerFactory.getLogger(StaleRunningRecoveryScheduler.class);
    private final StaleRunningRecoveryService recovery;
    private final MeterRegistry meters;

    @Scheduled(
            fixedDelayString = "${flowforge.execution.recovery.poll-interval-ms:60000}",
            initialDelayString = "${flowforge.execution.recovery.poll-initial-delay-ms:60000}")
    public void recover() {
        Timer.Sample sample = Timer.start(meters);
        String result = "failed";
        try {
            int count = recovery.recoverStaleNoopExecutions();
            // The service transaction has committed before this method returns.
            if (count > 0) {
                meters.counter("flowforge.execution.recovery.executions.total").increment(count);
                log.warn("Recovered {} stale NOOP workflow execution attempts", count);
            }
            result = "succeeded";
        } catch (RuntimeException ex) {
            log.error("Stale NOOP recovery failed; will retry next cycle", ex);
        } finally {
            meters.counter("flowforge.execution.recovery.cycles.total", "result", result).increment();
            sample.stop(meters.timer("flowforge.execution.recovery.cycle.duration", "result", result));
        }
    }
}
