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
