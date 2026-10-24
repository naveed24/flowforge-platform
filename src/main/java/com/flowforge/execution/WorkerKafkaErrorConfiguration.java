package com.flowforge.execution;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

/**
 * Poison dispatches are quarantined only after the DLT broker acknowledges
 * them. Transient database failures continue retrying without losing offsets.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
@ConditionalOnProperty(prefix = "flowforge.worker.consumer",
        name = "enabled", havingValue = "true")
public class WorkerKafkaErrorConfiguration {

    @Bean
    DefaultErrorHandler workerDispatchErrorHandler(
            KafkaTemplate<String, ExecutionDispatchMessage> kafka,
            @Value("${flowforge.worker.consumer.dlt-topic:flowforge.execution.dispatch.v1.DLT}") String dltTopic) {
        if (dltTopic == null || dltTopic.isBlank()) {
            throw new IllegalArgumentException("Worker dead-letter topic must not be blank");
        }

        // A negative partition lets Kafka choose a DLT partition; the DLT
        // does not need to mirror the dispatch topic's partition count.
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(dltTopic, -1));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));

        var handler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        // Invalid identity/payload is permanent. Database/Kafka outages are not.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        // The listener uses MANUAL_IMMEDIATE. Commit recovered offsets only
        // after the DLT send completes successfully.
        handler.setCommitRecovered(true);
        return handler;
    }
}
