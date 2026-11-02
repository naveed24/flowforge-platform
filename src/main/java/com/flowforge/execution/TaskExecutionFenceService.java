package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable admission and completion fence for future side-effecting task
 * executors. A row is unique per (execution, task), NOT per attempt: a retry
 * cannot replay an ambiguous external operation after a worker crash.
 *
 * Locking the parent execution row serializes first-claim, re-claim and
 * completion with other callers of this service. It also checks the current
 * RUNNING attempt and cancellation before admitting or completing work.
 *
 * IMPORTANT: this does not provide exactly-once remote side effects. Future
 * executors must pass the stable idempotency key to a cooperating external
 * service and reconcile IN_PROGRESS outcomes before attempting a replay.
 */
@Service
@RequiredArgsConstructor
public class TaskExecutionFenceService {
    private final WorkflowExecutionRepository executions;
    private final TaskExecutionCheckpointRepository checkpoints;
    private final Clock clock;

    public record Claim(long executionId, String taskKey, int attemptNumber,
                        UUID token, String idempotencyKey) {}

    /**
     * Empty means not currently admissible: wrong attempt, cancelled run,
     * completed task, or an unresolved IN_PROGRESS operation. Never infer
     * from an empty result that an external operation can safely be retried.
     */
    @Transactional
    public Optional<Claim> tryClaim(long executionId, String taskKey, int expectedAttempt) {
        validate(executionId, taskKey, expectedAttempt);
        var execution = executions.findByIdForUpdate(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown execution"));
        if (execution.getStatus() != ExecutionStatus.RUNNING
                || execution.getAttemptCount() != expectedAttempt) {
            return Optional.empty();
        }
        if (execution.getWorkflow().getTasks().stream()
                .noneMatch(task -> task.getTaskKey().equals(taskKey))) {
            throw new IllegalArgumentException("Task does not belong to execution workflow");
        }

        var existing = checkpoints.findByExecution_IdAndTaskKey(executionId, taskKey);
        UUID token = UUID.randomUUID();
        var now = clock.instant();
        if (existing.isPresent()) {
            var checkpoint = existing.orElseThrow();
            // IN_PROGRESS is intentionally not lease-expired automatically:
            // a remote side effect may already have happened.
            if (checkpoint.getState() != TaskExecutionCheckpoint.State.RETRYABLE
                    || checkpoint.getAttemptNumber() >= expectedAttempt) {
                return Optional.empty();
            }
            checkpoint.reclaim(expectedAttempt, token, now);
            checkpoints.saveAndFlush(checkpoint);
        } else {
            checkpoints.saveAndFlush(new TaskExecutionCheckpoint(
                    execution, taskKey, expectedAttempt, token, now));
        }
        return Optional.of(new Claim(executionId, taskKey, expectedAttempt,
                token, stableIdempotencyKey(executionId, taskKey)));
    }

    /** Commit success only for the currently RUNNING attempt and exact token. */
    @Transactional
    public boolean succeed(Claim claim) {
        return transition(claim, TaskExecutionCheckpoint.State.SUCCEEDED);
    }

    /**
     * Use ONLY when the external operation definitely did not take effect.
     * Timeouts, connection loss and unknown remote outcomes are NOT retryable:
     * leave IN_PROGRESS for explicit reconciliation instead.
     */
    @Transactional
    public boolean markRetryable(Claim claim) {
        return transition(claim, TaskExecutionCheckpoint.State.RETRYABLE);
    }

    private boolean transition(Claim claim, TaskExecutionCheckpoint.State target) {
        if (claim == null || claim.executionId() <= 0 || claim.attemptNumber() <= 0
                || claim.taskKey() == null || claim.token() == null) {
            throw new IllegalArgumentException("Valid task claim is required");
        }
        var execution = executions.findByIdForUpdate(claim.executionId());
        if (execution.isEmpty() || execution.orElseThrow().getStatus() != ExecutionStatus.RUNNING
                || execution.orElseThrow().getAttemptCount() != claim.attemptNumber()) {
            return false;
        }
        var checkpoint = checkpoints.findByExecution_IdAndTaskKey(
                claim.executionId(), claim.taskKey());
        if (checkpoint.isEmpty()) return false;
        var row = checkpoint.orElseThrow();
        if (row.getState() != TaskExecutionCheckpoint.State.IN_PROGRESS
                || row.getAttemptNumber() != claim.attemptNumber()
                || !row.getClaimToken().equals(claim.token())) {
            return false;
        }
        row.transition(target, clock.instant());
        checkpoints.saveAndFlush(row);
        return true;
    }

    private static void validate(long executionId, String taskKey, int attempt) {
        if (executionId <= 0 || attempt <= 0 || taskKey == null
                || taskKey.isBlank() || taskKey.length() > 120) {
            throw new IllegalArgumentException("Invalid execution, task key or attempt");
        }
    }

    private static String stableIdempotencyKey(long executionId, String taskKey) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    (executionId + ":" + taskKey).getBytes(StandardCharsets.UTF_8));
            return "ff-" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by the JVM", impossible);
        }
    }
}
