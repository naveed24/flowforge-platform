package com.flowforge.execution;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

/**
 * Internal-only reconciliation primitive. Do not expose without operator
 * authentication, authorization and independent remote-outcome verification.
 * A caller must provide evidence of the remote result, not just a timeout.
 */
@Service
@RequiredArgsConstructor
public class TaskCheckpointReconciliationService {
    private final WorkflowExecutionRepository executions;
    private final TaskExecutionCheckpointRepository checkpoints;
    private final TaskCheckpointReconciliationAuditRepository audit;
    private final Clock clock;

    public enum VerifiedOutcome { REMOTE_SUCCEEDED, REMOTE_DID_NOT_EXECUTE }

    /**
     * Expected claim token/attempt are a compare-and-set fence. This can
     * reconcile a previous attempt even when its execution has stopped.
     * No workflow state transition or automatic dispatch is performed.
     */
    @Transactional
    public boolean reconcile(long executionId, String taskKey, int expectedAttempt,
            UUID expectedToken, VerifiedOutcome outcome, String evidenceReference) {
        if (executionId <= 0 || taskKey == null || taskKey.isBlank()
                || expectedAttempt <= 0 || expectedToken == null || outcome == null
                || evidenceReference == null) {
            throw new IllegalArgumentException("Valid checkpoint identity and verified evidence are required");
        }
        String evidence = validateEvidenceReference(evidenceReference);
        // Lock the same parent row as worker admission/completion.
        if (executions.findByIdForUpdate(executionId).isEmpty()) return false;
        var found = checkpoints.findByExecution_IdAndTaskKey(executionId, taskKey);
        if (found.isEmpty()) return false;
        var checkpoint = found.orElseThrow();
        if (checkpoint.getState() != TaskExecutionCheckpoint.State.IN_PROGRESS
                || checkpoint.getAttemptNumber() != expectedAttempt
                || !checkpoint.getClaimToken().equals(expectedToken)) return false;

        var target = outcome == VerifiedOutcome.REMOTE_SUCCEEDED
                ? TaskExecutionCheckpoint.State.SUCCEEDED
                : TaskExecutionCheckpoint.State.RETRYABLE;
        var now = clock.instant();
        audit.saveAndFlush(new TaskCheckpointReconciliationAudit(
                checkpoint, target, evidence, now));
        checkpoint.transition(target, now);
        checkpoints.saveAndFlush(checkpoint);
        return true;
    }
    /** Reject ambiguous, multiline or invisible evidence identifiers before persisting an audit. */
    static String validateEvidenceReference(String raw) {
        String evidence = Objects.requireNonNull(raw, "Evidence reference is required").strip();
        if (evidence.isEmpty() || evidence.length() > 512 || evidence.codePoints().anyMatch(cp ->
                Character.isISOControl(cp) || Character.getType(cp) == Character.FORMAT
                        || Character.getType(cp) == Character.LINE_SEPARATOR
                        || Character.getType(cp) == Character.PARAGRAPH_SEPARATOR)) {
            throw new IllegalArgumentException("Evidence reference must be 1-512 visible characters without controls");
        }
        return evidence;
    }
}
