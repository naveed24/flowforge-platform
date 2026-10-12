-- Support newest-first paginated history, with deterministic ID tie-breaking.
CREATE INDEX idx_workflow_executions_history
    ON workflow_executions (workflow_id, created_at DESC, id DESC);

-- Keep status-filtered history scans bounded within one workflow.
CREATE INDEX idx_workflow_executions_history_status
    ON workflow_executions (workflow_id, status, created_at DESC, id DESC);
