package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * First worker execution slice: complete NOOP-only DAGs atomically.
 * Claims and completion share one database transaction, so a crash before
 * commit leaves the attempt QUEUED and Kafka can safely redeliver it.
 */
@Service
@RequiredArgsConstructor
public class NoopWorkflowDispatchService {
    private final ExecutionDispatchOutboxRepository outbox;
    private final ExecutionClaimService claims;
    private final ExecutionAttemptCompletionService completion;

    public enum Outcome { SUCCEEDED, UNSUPPORTED_RETRY, DUPLICATE_OR_STALE }

    @Transactional
    public Outcome process(ExecutionDispatchMessage event) {
        if (event == null || event.executionId() <= 0
                || event.attemptNumber() <= 0 || event.outboxId() <= 0) {
            throw new IllegalArgumentException("Invalid dispatch event");
        }
        var intent = outbox.findById(event.outboxId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown dispatch outbox ID"));
        if (intent.getExecution().getId() != event.executionId()
                || intent.getAttemptNumber() != event.attemptNumber()) {
            throw new IllegalArgumentException("Dispatch event does not match its outbox intent");
        }
        var claimed = claims.claimQueuedById(event.executionId(), event.attemptNumber());
        if (claimed.isEmpty()) return Outcome.DUPLICATE_OR_STALE;

        var tasks = claimed.orElseThrow().getWorkflow().getTasks();
        boolean supported = !tasks.isEmpty()
                && tasks.stream().allMatch(task -> task.getTaskType() == TaskType.NOOP);
        boolean transitioned = supported
                ? completion.succeed(event.executionId(), event.attemptNumber())
                : completion.fail(event.executionId(), event.attemptNumber());
        if (!transitioned) throw new IllegalStateException("Claimed execution could not be completed");
        return supported ? Outcome.SUCCEEDED : Outcome.UNSUPPORTED_RETRY;
    }
}
