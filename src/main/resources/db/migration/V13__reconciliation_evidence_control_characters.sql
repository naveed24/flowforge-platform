-- Reject control characters in reconciliation evidence at the storage boundary.
-- In particular, CR/LF/NUL and other control bytes must never enter an audit
-- reference through direct SQL writes that bypass service validation.
-- PostgreSQL validates existing rows when this constraint is added; deployments
-- with invalid historical evidence fail closed and require manual remediation.
ALTER TABLE task_checkpoint_reconciliation_audit
    ADD CONSTRAINT chk_task_reconciliation_evidence_no_controls
        CHECK (evidence_reference !~ '[[:cntrl:]]');
