CREATE TABLE workflows (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(140) NOT NULL UNIQUE,
    description VARCHAR(800),
    status VARCHAR(30) NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE workflow_tasks (
    id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL,
    task_key VARCHAR(120) NOT NULL,
    task_type VARCHAR(30) NOT NULL,
    CONSTRAINT fk_workflow_tasks_workflow
        FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    CONSTRAINT uk_task_workflow_key UNIQUE (workflow_id, task_key)
);

CREATE TABLE workflow_task_dependencies (
    task_id BIGINT NOT NULL,
    depends_on_task_key VARCHAR(120) NOT NULL,
    CONSTRAINT fk_task_dependency_task
        FOREIGN KEY (task_id) REFERENCES workflow_tasks(id) ON DELETE CASCADE
);

CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_workflow_tasks_workflow_id ON workflow_tasks(workflow_id);
CREATE INDEX idx_task_dependencies_task_id ON workflow_task_dependencies(task_id);
