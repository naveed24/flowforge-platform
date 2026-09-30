ALTER TABLE workflows
    ADD COLUMN schedule_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    ADD COLUMN cron_expression VARCHAR(120),
    ADD COLUMN schedule_timezone VARCHAR(64) NOT NULL DEFAULT 'UTC';

CREATE INDEX idx_workflows_schedule_type ON workflows(schedule_type);
