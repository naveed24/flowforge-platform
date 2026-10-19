-- A committed QUEUED transition and its dispatch intent must be durable together.
-- Publishers are added separately; do not delete or mark these rows before Kafka acknowledges.
CREATE TABLE execution_dispatch_outbox (
    id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL,
    attempt_number INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT fk_execution_dispatch_outbox_execution
        FOREIGN KEY (execution_id) REFERENCES workflow_executions(id),
    CONSTRAINT uq_execution_dispatch_outbox_attempt UNIQUE (execution_id, attempt_number),
    CONSTRAINT chk_execution_dispatch_outbox_attempt CHECK (attempt_number >= 1)
);

CREATE INDEX idx_execution_dispatch_outbox_pending
    ON execution_dispatch_outbox (published_at, id);

-- Backfill only currently queued runs from deployments preceding this migration.
-- Historical completed/RUNNING executions must not be redispatched.
INSERT INTO execution_dispatch_outbox (execution_id, attempt_number, created_at)
SELECT id, attempt_count + 1, created_at
FROM workflow_executions
WHERE status = 'QUEUED' AND attempt_count < max_attempts;
