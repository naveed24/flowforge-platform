package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

/**
 * Worker-facing attempt-fenced completion. Callers must retain the attempt
 * number from their successful claim, not read a newer number for old work.
 */
@Service
@RequiredArgsConstructor
public class ExecutionAttemptCompletionService {

    private final WorkflowExecutionRepository repository;
    private final Clock clock;

    /** True only when this RUNNING attempt was transitioned to SUCCEEDED. */
    @Transactional
    public boolean succeed(long executionId, int expectedAttempt) {
        if (executionId <= 0 || expectedAttempt <= 0) return false;
        return repository.completeRunningAttempt(
                executionId, expectedAttempt, ExecutionStatus.RUNNING,
                ExecutionStatus.SUCCEEDED, null, clock.instant()) == 1;
    }

    /**
     * Persist retry backoff or terminal failure, atomically and at most once
     * for the specified attempt. Stale and cancelled attempts are no-ops.
     */
    @Transactional
    public boolean fail(long executionId, int expectedAttempt) {
        if (executionId <= 0 || expectedAttempt <= 0) return false;
        var candidate = repository.findById(executionId);
        if (candidate.isEmpty()) return false;
        var execution = candidate.get();
        if (execution.getStatus() != ExecutionStatus.RUNNING
                || execution.getAttemptCount() != expectedAttempt) return false;

        Instant now = clock.instant();
        boolean retry = expectedAttempt < execution.getMaxAttempts();
        return repository.completeRunningAttempt(
                executionId, expectedAttempt, ExecutionStatus.RUNNING,
                retry ? ExecutionStatus.RETRY_WAIT : ExecutionStatus.FAILED,
                retry ? now.plus(RetryBackoffPolicy.delayAfterFailure(expectedAttempt)) : null,
                retry ? null : now) == 1;
    }
}
