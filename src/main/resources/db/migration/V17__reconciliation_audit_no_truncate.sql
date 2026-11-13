-- Row-level UPDATE/DELETE triggers do not fire for TRUNCATE. Close that
-- bypass for reconciliation evidence, including TRUNCATE on an empty table.
-- This is defense in depth, not protection from database owners/superusers.
CREATE TRIGGER trg_reconciliation_audit_no_truncate
BEFORE TRUNCATE ON task_checkpoint_reconciliation_audit
FOR EACH STATEMENT
EXECUTE FUNCTION reject_reconciliation_audit_mutation();
