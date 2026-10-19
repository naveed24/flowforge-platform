package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/**
 * Inserts the dispatch intent in the SAME transaction as the QUEUED state
 * transition. Never call this after committing an execution.
 */
@Service
@RequiredArgsConstructor
public class ExecutionDispatchOutboxService {

    private final ExecutionDispatchOutboxRepository repository;
    private final Clock clock;

    @Transactional(propagation = Propagation.MANDATORY)
    public void recordForNextAttempt(WorkflowExecution execution) {
        if (execution.getStatus() != ExecutionStatus.QUEUED
                || execution.getAttemptCount() >= execution.getMaxAttempts()) {
            throw new IllegalStateException("Dispatch intent requires a claimable queued execution");
        }
        repository.saveAndFlush(new ExecutionDispatchOutbox(
                execution, execution.getAttemptCount() + 1, clock.instant()));
    }
}
