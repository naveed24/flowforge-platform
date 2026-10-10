-- Existing executions and requests without a key remain supported.
ALTER TABLE workflow_executions
    ADD COLUMN idempotency_key VARCHAR(128);

-- A nullable key permits multiple keyless runs; non-null keys are workflow-scoped.
CREATE UNIQUE INDEX uq_workflow_executions_idempotency
    ON workflow_executions (workflow_id, idempotency_key);
