-- Outbox publication claim lease migration
ALTER TABLE execution_dispatch_outbox ADD COLUMN claim_token UUID, ADD COLUMN claimed_until TIMESTAMPTZ, ADD COLUMN next_publish_at TIMESTAMPTZ, ADD COLUMN publish_attempts INTEGER NOT NULL DEFAULT 0, ADD COLUMN last_error VARCHAR(120);
ALTER TABLE execution_dispatch_outbox ADD CONSTRAINT chk_execution_dispatch_claim_pair CHECK ((claim_token IS NULL) = (claimed_until IS NULL)), ADD CONSTRAINT chk_execution_dispatch_publish_attempts CHECK (publish_attempts >= 0);
CREATE INDEX idx_execution_dispatch_outbox_claimable ON execution_dispatch_outbox (published_at, next_publish_at, claimed_until, id);
