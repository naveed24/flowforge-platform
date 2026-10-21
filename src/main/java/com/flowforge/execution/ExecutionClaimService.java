package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;

/**
 * Database-backed worker claim primitive. This deliberately does not execute
 * tasks or publish Kafka messages; those are separate delivery milestones.
 */
@Service
@RequiredArgsConstructor
public class ExecutionClaimService {

    private static final int SCAN_LIMIT = 100;

    private final WorkflowExecutionRepository repository;
    private final Clock clock;

    /**
     * Returns at most one execution newly transitioned from QUEUED to RUNNING.
     * Safe across multiple application instances: a conditional SQL UPDATE is
     * the claim's linearization point. Never returns an already-running job.
     */
    /** Claim one queued execution addressed by an at-least-once delivery. */
    @Transactional
    public Optional<WorkflowExecution> claimQueuedById(long executionId) {
        if (executionId <= 0 || repository.claimQueuedExecution(
                executionId, ExecutionStatus.QUEUED, ExecutionStatus.RUNNING,
                clock.instant()) != 1) {
            return Optional.empty();
        }
        return repository.findById(executionId);
    }

    /**
     * Kafka consumers must supply the event's attempt. A delayed event for
     * attempt N must not claim a newly queued attempt N+1.
     */
    @Transactional
    public Optional<WorkflowExecution> claimQueuedById(long executionId, int expectedAttempt) {
        if (executionId <= 0 || expectedAttempt <= 0
                || repository.claimQueuedExecutionForAttempt(
                executionId, expectedAttempt - 1, ExecutionStatus.QUEUED,
                ExecutionStatus.RUNNING, clock.instant()) != 1) {
            return Optional.empty();
        }
        return repository.findById(executionId);
    }

    @Transactional
    public Optional<WorkflowExecution> claimNextQueued() {
        var candidates = repository.findClaimableQueuedIds(
                ExecutionStatus.QUEUED, PageRequest.of(0, SCAN_LIMIT));
        for (Long id : candidates) {
            if (repository.claimQueuedExecution(
                    id, ExecutionStatus.QUEUED, ExecutionStatus.RUNNING, clock.instant()) == 1) {
                return repository.findById(id);
            }
        }
        return Optional.empty();
    }
}
