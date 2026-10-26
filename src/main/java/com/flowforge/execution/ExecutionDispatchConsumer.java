package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Opt-in Kafka consumer. Acknowledges offsets only after committed worker
 * processing and, when enabled, successful Redis lease verification/release.
 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "flowforge.worker.consumer",
        name = "enabled", havingValue = "true")
public class ExecutionDispatchConsumer {
    private static final Logger log = LoggerFactory.getLogger(ExecutionDispatchConsumer.class);
    private final WorkerDispatchLeaseCoordinator worker;

    @KafkaListener(topics = "${flowforge.execution.dispatch.topic:flowforge.execution.dispatch.v1}",
            groupId = "${flowforge.worker.consumer.group-id:flowforge-workers}")
    public void onMessage(ConsumerRecord<String, ExecutionDispatchMessage> record,
                          Acknowledgment acknowledgment) {
        if (record == null || record.value() == null) {
            throw new IllegalArgumentException("Dispatch record and payload are required");
        }
        var event = record.value();
        if (event.executionId() <= 0 || event.attemptNumber() <= 0 || event.outboxId() <= 0
                || !Long.toString(event.executionId()).equals(record.key())) {
            throw new IllegalArgumentException("Invalid dispatch identity or attempt");
        }
        var outcome = worker.process(event);
        if (outcome == NoopWorkflowDispatchService.Outcome.UNSUPPORTED_RETRY) {
            log.warn("Unsupported tasks in execution {} attempt {}",
                    event.executionId(), event.attemptNumber());
        }
        acknowledgment.acknowledge();
    }
}
