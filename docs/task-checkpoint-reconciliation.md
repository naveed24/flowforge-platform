# Manual task checkpoint reconciliation

This is an **internal service**, not a public HTTP endpoint or automatic remote-result verifier. A worker crash can leave an `IN_PROGRESS` checkpoint even when the remote operation completed. Operators must independently inspect the remote system using its stable idempotency key and record a durable evidence reference before invoking `TaskCheckpointReconciliationService.reconcile`.

- `REMOTE_SUCCEEDED`: remote operation is independently confirmed; checkpoint becomes `SUCCEEDED` and cannot be replayed.
- `REMOTE_DID_NOT_EXECUTE`: remote operation is conclusively known not to have occurred; checkpoint becomes `RETRYABLE`. Only a **later RUNNING execution attempt** can claim it again.
- Unknown, timeout, partial, or contradictory evidence: **do not reconcile**. Leave the checkpoint `IN_PROGRESS`.

The service requires the checkpoint's exact attempt and UUID claim token. It locks the parent execution row, compares current checkpoint state and ownership, and writes an audit row in the same database transaction as the transition. Repeated/stale reconciliation fails without a new audit row. Evidence is a reference to independent proof, not proof by itself. The audit is retained with the checkpoint; deleting an execution cascades its checkpoint and audit history. Protect backups and access accordingly.

Reconciliation does not change execution status, automatically restart a failed/cancelled workflow, authorize remote calls, or provide exactly-once semantics. **Do not expose this internal service to users without authentication, authorization, approval controls, and audit retention policy.**
