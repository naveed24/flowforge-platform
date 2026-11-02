-- One durable checkpoint per execution/task across all retry generations.
-- An uncertain external side effect remains IN_PROGRESS until reconciled;
-- retries must never silently claim it again.
CREATE TABLE execution_task_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL,
    task_key VARCHAR(120) NOT NULL,
    state VARCHAR(20) NOT NULL,
    attempt_number INTEGER NOT NULL,
    claim_token UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_execution_task_checkpoints_execution
        FOREIGN KEY (execution_id) REFERENCES workflow_executions(id) ON DELETE CASCADE,
    CONSTRAINT uq_execution_task_checkpoints_task UNIQUE (execution_id, task_key),
    CONSTRAINT chk_execution_task_checkpoints_state
        CHECK (state IN ('IN_PROGRESS', 'RETRYABLE', 'SUCCEEDED')),
    CONSTRAINT chk_execution_task_checkpoints_attempt CHECK (attempt_number >= 1)
);

CREATE INDEX idx_execution_task_checkpoints_state
    ON execution_task_checkpoints (state, updated_at);
