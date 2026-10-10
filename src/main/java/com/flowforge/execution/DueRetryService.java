package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.Clock;

/**
 * Moves due retries into the normal QUEUED state. This does not execute tasks;
 * dispatch/worker claiming is a separate concern.
 */
@Service
@RequiredArgsConstructor
public class DueRetryService {

    private static final int BATCH_SIZE = 100;

    private final WorkflowExecutionRepository repository;
    private final Clock clock;

    public int requeueDueRetries() {
        var now = clock.instant();
        var ids = repository.findDueRetryIds(
                ExecutionStatus.RETRY_WAIT, now, PageRequest.of(0, BATCH_SIZE));

        int requeued = 0;
        for (Long id : ids) {
            requeued += repository.requeueDueRetry(
                    id, ExecutionStatus.RETRY_WAIT, ExecutionStatus.QUEUED, now);
        }
        return requeued;
    }
}
