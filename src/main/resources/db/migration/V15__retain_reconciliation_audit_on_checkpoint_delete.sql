-- Reconciliation evidence must survive routine execution/checkpoint cleanup.
-- The V10 foreign key used ON DELETE CASCADE, silently deleting audit records
-- whenever their checkpoint (or its parent execution) was removed.
-- Fail closed instead: a checkpoint with audit history cannot be deleted until
-- an explicitly authorized retention process handles its evidence.
-- RESTRICT is intentional (not SET NULL): audit rows always retain their
-- referential link to the original checkpoint.
ALTER TABLE task_checkpoint_reconciliation_audit
    DROP CONSTRAINT task_checkpoint_reconciliation_audit_checkpoint_id_fkey;

ALTER TABLE task_checkpoint_reconciliation_audit
    ADD CONSTRAINT fk_task_reconciliation_checkpoint_retained
        FOREIGN KEY (checkpoint_id)
        REFERENCES execution_task_checkpoints(id)
        ON DELETE RESTRICT;
