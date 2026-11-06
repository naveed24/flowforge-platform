-- A checkpoint claim may be reconciled at most once, even if a future caller
-- accidentally bypasses the service's parent-row serialization.
-- Attempts can be retried with a new claim token; those are distinct claims.
CREATE UNIQUE INDEX uq_task_checkpoint_reconciliation_claim
    ON task_checkpoint_reconciliation_audit(checkpoint_id, previous_attempt, previous_claim_token);
