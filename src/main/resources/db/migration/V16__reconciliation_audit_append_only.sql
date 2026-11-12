-- Reconciliation decisions are append-only evidence. Prevent ordinary SQL writes
-- from silently rewriting or removing an audit decision after it is recorded.
-- Any future retention/export process must be separately reviewed and must
-- explicitly account for this guard; no bypass is provided by the application.
CREATE FUNCTION reject_reconciliation_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'reconciliation audit history is append-only'
        USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER trg_reconciliation_audit_append_only
BEFORE UPDATE OR DELETE ON task_checkpoint_reconciliation_audit
FOR EACH ROW
EXECUTE FUNCTION reject_reconciliation_audit_mutation();
