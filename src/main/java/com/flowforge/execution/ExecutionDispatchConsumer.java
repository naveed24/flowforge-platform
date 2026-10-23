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
 * Opt-in Kafka consumer. Acknowledges offsets only after database commit.
 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "flowforge.worker.consumer",
        name = "enabled", havingValue = "true")
public class ExecutionDispatchConsumer {
    private static final Logger log = LoggerFactory.getLogger(ExecutionDispatchConsumer.class);
    private final NoopWorkflowDispatchService worker;

    @KafkaListener(topics = "${flowforge.execution.dispatch.topic:flowforge.execution.dispatch.v1}",
            groupId = "${flowforge.worker.consumer.group-id:flowforge-workers}")
    public void onMessage(ConsumerRecord<String, ExecutionDispatchMessage> record,
                          Acknowledgment acknowledgment) {
        if (record == null || record.value() == null
                || !Long.toString(record.value().executionId()).equals(record.key())) {
            throw new IllegalArgumentException("Dispatch key must match execution ID");
        }
        var outcome = worker.process(record.value());
        if (outcome == NoopWorkflowDispatchService.Outcome.UNSUPPORTED_RETRY) {
            log.warn("Unsupported tasks in execution {} attempt {}",
                    record.value().executionId(), record.value().attemptNumber());
        }
        acknowledgment.acknowledge();
    }
}
