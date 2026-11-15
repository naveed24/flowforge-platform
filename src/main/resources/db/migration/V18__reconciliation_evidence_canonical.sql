-- Prevent direct SQL writers from bypassing the service's canonical trim.
-- Reject existing noncanonical evidence rather than rewriting audit history.
ALTER TABLE task_checkpoint_reconciliation_audit
    ADD CONSTRAINT chk_task_reconciliation_evidence_trimmed
        CHECK (evidence_reference = btrim(evidence_reference));
