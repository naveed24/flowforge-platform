package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Disabled until a Kafka broker/topic is configured explicitly. */
@Component
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "flowforge.execution.dispatch",
        name = "enabled", havingValue = "true")
public class ExecutionDispatchScheduler {
    private static final Logger log = LoggerFactory.getLogger(ExecutionDispatchScheduler.class);
    private final ExecutionDispatchPublisher publisher;

    @Scheduled(fixedDelayString = "${flowforge.execution.dispatch.poll-interval-ms:2000}",
            initialDelayString = "${flowforge.execution.dispatch.poll-initial-delay-ms:5000}")
    public void publish() {
        try {
            int count = publisher.publishPendingBatch();
            if (count > 0) log.info("Published {} workflow execution dispatch events", count);
        } catch (RuntimeException ex) {
            log.error("Dispatch poll failed; pending rows will be retried", ex);
        }
    }
}
