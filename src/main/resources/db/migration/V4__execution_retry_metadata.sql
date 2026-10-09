-- Existing executions remain queued or terminal; new executions start with three total attempts.
ALTER TABLE workflow_executions
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN next_attempt_at TIMESTAMPTZ;

ALTER TABLE workflow_executions
    ADD CONSTRAINT chk_workflow_executions_attempts
    CHECK (attempt_count >= 0 AND max_attempts >= 1 AND attempt_count <= max_attempts);

CREATE INDEX idx_workflow_executions_retry_due
    ON workflow_executions(status, next_attempt_at);
