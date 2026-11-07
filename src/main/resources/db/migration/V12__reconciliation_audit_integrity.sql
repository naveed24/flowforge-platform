-- Guard the immutable reconciliation evidence trail against direct SQL writers
-- that bypass application-level validation. Existing records must satisfy these
-- invariants; migration fails rather than silently rewriting audit evidence.
ALTER TABLE task_checkpoint_reconciliation_audit
    ADD CONSTRAINT chk_task_reconciliation_previous_attempt
        CHECK (previous_attempt >= 1),
    ADD CONSTRAINT chk_task_reconciliation_evidence_not_blank
        CHECK (length(btrim(evidence_reference)) > 0);
