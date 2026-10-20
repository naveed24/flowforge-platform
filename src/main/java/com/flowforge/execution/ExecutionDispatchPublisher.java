package com.flowforge.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * At-least-once outbox publisher. Kafka acknowledgment precedes the database
 * published marker. Duplicate delivery after a crash is expected and safe only
 * when consumers use the attempt-aware execution claim.
 */
@Service
public class ExecutionDispatchPublisher {
    private static final Logger log = LoggerFactory.getLogger(ExecutionDispatchPublisher.class);
    private final ExecutionDispatchClaimService claims;
    private final KafkaTemplate<String, ExecutionDispatchMessage> kafka;
    private final String topic;
    private final long ackTimeoutMillis;
    private final int batchSize;

    public ExecutionDispatchPublisher(
            ExecutionDispatchClaimService claims,
            KafkaTemplate<String, ExecutionDispatchMessage> kafka,
            @Value("${flowforge.execution.dispatch.topic:flowforge.execution.dispatch.v1}") String topic,
            @Value("${flowforge.execution.dispatch.ack-timeout-ms:10000}") long ackTimeoutMillis,
            @Value("${flowforge.execution.dispatch.claim-lease-ms:60000}") long claimLeaseMillis,
            @Value("${flowforge.execution.dispatch.batch-size:25}") int batchSize) {
        if (topic == null || topic.isBlank() || ackTimeoutMillis < 1000
                || ackTimeoutMillis >= claimLeaseMillis || batchSize < 1 || batchSize > 100) {
            throw new IllegalArgumentException("Invalid dispatch topic, timeout, lease or batch size");
        }
        this.claims = claims;
        this.kafka = kafka;
        this.topic = topic;
        this.ackTimeoutMillis = ackTimeoutMillis;
        this.batchSize = batchSize;
    }

    /** One bounded polling cycle; no database transaction spans Kafka I/O. */
    public int publishPendingBatch() {
        int published = 0;
        for (int i = 0; i < batchSize; i++) {
            var next = claims.claimNext();
            if (next.isEmpty()) break;
            var claim = next.orElseThrow();
            var event = new ExecutionDispatchMessage(
                    claim.executionId(), claim.attemptNumber(), claim.outboxId());
            try {
                kafka.send(topic, Long.toString(claim.executionId()), event)
                        .get(ackTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                claims.releaseForRetry(claim, ex.getClass().getSimpleName());
                break;
            } catch (ExecutionException | TimeoutException | RuntimeException ex) {
                if (!claims.releaseForRetry(claim, ex.getClass().getSimpleName())) {
                    log.warn("Lost dispatch claim for outbox row {} after publish failure", claim.outboxId());
                }
                log.warn("Kafka publish failed for outbox row {}; retry scheduled", claim.outboxId(), ex);
                continue;
            }
            if (claims.markPublished(claim)) {
                published++;
            } else {
                log.warn("Outbox claim {} expired or changed after Kafka acknowledgment", claim.outboxId());
            }
        }
        return published;
    }
}
