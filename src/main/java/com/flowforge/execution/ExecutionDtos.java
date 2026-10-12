package com.flowforge.execution;

import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Page;

public final class ExecutionDtos {

    private ExecutionDtos() {}

    public record CancelExecutionRequest(@NotNull Long expectedVersion) {}

    public record ExecutionResponse(
            Long id,
            Long workflowId,
            ExecutionStatus status,
            Long version,
            int attemptCount,
            int maxAttempts,
            Instant nextAttemptAt,
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
                    execution.getAttemptCount(),
                    execution.getMaxAttempts(),
                    execution.getNextAttemptAt(),
                    execution.getCreatedAt(),
                    execution.getStartedAt(),
                    execution.getFinishedAt()
            );
        }
    }
    /** Stable API contract rather than exposing Spring Data's Page internals. */
    public record ExecutionPageResponse(
            List<ExecutionResponse> items,
            int page,
            int size,
            long totalElements,
            int totalPages,
            boolean hasNext
    ) {
        static ExecutionPageResponse from(Page<ExecutionResponse> result) {
            return new ExecutionPageResponse(
                    result.getContent(), result.getNumber(), result.getSize(),
                    result.getTotalElements(), result.getTotalPages(), result.hasNext());
        }
    }
}
