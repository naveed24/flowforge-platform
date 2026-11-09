-- Enforce the evidence identifier size limit at the storage boundary as well as
-- in the reconciliation service. Direct SQL writers must not be able to store
-- unbounded audit evidence. Existing invalid rows fail migration validation;
-- do not silently truncate or rewrite immutable evidence.
ALTER TABLE task_checkpoint_reconciliation_audit
    ADD CONSTRAINT chk_task_reconciliation_evidence_max_length
        CHECK (char_length(evidence_reference) <= 512);
