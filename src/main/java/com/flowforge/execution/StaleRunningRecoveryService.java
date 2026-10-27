package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/**
 * Opt-in recovery of orphaned RUNNING attempts for NOOP-only workflows.
 * A stale timestamp is not proof of a crash, so non-NOOP workflows are
 * deliberately excluded until task-level fencing and cancellation exist.
 */
@Service
public class StaleRunningRecoveryService {
    private static final int BATCH_SIZE = 100;
    private static final long MIN_TIMEOUT_MS = 60_000;
    private static final long MAX_TIMEOUT_MS = 86_400_000;

    private final WorkflowExecutionRepository repository;
    private final StringRedisTemplate redis;
    private final Clock clock;
    private final long timeoutMs;
    private final boolean leaseEnabled;

    public StaleRunningRecoveryService(
            WorkflowExecutionRepository repository,
            StringRedisTemplate redis,
            Clock clock,
            @Value("${flowforge.execution.recovery.timeout-ms:900000}") long timeoutMs,
            @Value("${flowforge.worker.consumer.lease-enabled:false}") boolean leaseEnabled) {
        if (timeoutMs < MIN_TIMEOUT_MS || timeoutMs > MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException("Stale recovery timeout must be 60000..86400000 ms");
        }
        this.repository = repository;
        this.redis = redis;
        this.clock = clock;
        this.timeoutMs = timeoutMs;
        this.leaseEnabled = leaseEnabled;
    }

    @Transactional
    public int recoverStaleNoopExecutions() {
        var now = clock.instant();
        var cutoff = now.minusMillis(timeoutMs);
        var ids = repository.findStaleNoopRunningIds(
                ExecutionStatus.RUNNING, TaskType.NOOP, cutoff, PageRequest.of(0, BATCH_SIZE));

        int recovered = 0;
        for (Long id : ids) {
            // When worker leasing is enabled, do not reclaim a live owner's
            // execution. Redis outages or unknown responses fail closed.
            if (leaseEnabled) {
                Boolean exists = redis.hasKey("flowforge:execution:lease:" + id);
                if (!Boolean.FALSE.equals(exists)) {
                    continue;
                }
            }
            var candidate = repository.findById(id).orElseThrow();
            int attempt = candidate.getAttemptCount();
            if (attempt < 1) continue;

            boolean retry = attempt < candidate.getMaxAttempts();
            var nextAttempt = retry ? now.plus(RetryBackoffPolicy.delayAfterFailure(attempt)) : null;
            var finished = retry ? null : now;
            int updated = repository.recoverStaleNoopRunningAttempt(
                    id, attempt, ExecutionStatus.RUNNING,
                    retry ? ExecutionStatus.RETRY_WAIT : ExecutionStatus.FAILED,
                    TaskType.NOOP, cutoff, nextAttempt, finished);
            recovered += updated;
        }
        return recovered;
    }
}
