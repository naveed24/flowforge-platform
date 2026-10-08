package com.flowforge.execution;

import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public final class ExecutionDtos {

    private ExecutionDtos() {}

    public record CancelExecutionRequest(@NotNull Long expectedVersion) {}

    public record ExecutionResponse(
            Long id,
            Long workflowId,
            ExecutionStatus status,
            Long version,
            Instant createdAt,
            Instant startedAt,
            Instant finishedAt
    ) {
        static ExecutionResponse from(WorkflowExecution execution) {
            return new ExecutionResponse(
                    execution.getId(),
                    execution.getWorkflow().getId(),
                    execution.getStatus(),
                    execution.getVersion(),
                    execution.getCreatedAt(),
                    execution.getStartedAt(),
                    execution.getFinishedAt()
            );
        }
    }
}
