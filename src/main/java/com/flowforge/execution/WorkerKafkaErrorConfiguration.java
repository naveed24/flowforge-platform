package com.flowforge.execution;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
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
            @Value("${flowforge.worker.consumer.dlt-topic:flowforge.execution.dispatch.v1.DLT}") String dltTopic,
            MeterRegistry meters) {
        if (dltTopic == null || dltTopic.isBlank()) {
            throw new IllegalArgumentException("Worker dead-letter topic must not be blank");
        }

        // A negative partition lets Kafka choose a DLT partition; the DLT
        // does not need to mirror the dispatch topic's partition count.
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(dltTopic, -1));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));

        // Only count a published DLT record after the recoverer has waited for
        // broker acknowledgment. If the send fails, propagate the exception so
        // the original Kafka offset remains eligible for redelivery.
        for (String result : new String[] {"published", "failed"}) {
            meters.counter("flowforge.worker.dead_letter.publish", "result", result);
            meters.timer("flowforge.worker.dead_letter.publish.duration", "result", result);
        }
        ConsumerRecordRecoverer observedRecoverer = (record, error) -> {
            Timer.Sample sample = Timer.start(meters);
            String result = "failed";
            try {
                recoverer.accept(record, error);
                result = "published";
            } finally {
                meters.counter("flowforge.worker.dead_letter.publish", "result", result).increment();
                sample.stop(meters.timer(
                        "flowforge.worker.dead_letter.publish.duration", "result", result));
            }
        };

        var handler = new DefaultErrorHandler(observedRecoverer,
                new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        // Invalid identity/payload is permanent. Database/Kafka outages are not.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        // The listener uses MANUAL_IMMEDIATE. Commit recovered offsets only
        // after the DLT send completes successfully.
        handler.setCommitRecovered(true);
        return handler;
    }
}
