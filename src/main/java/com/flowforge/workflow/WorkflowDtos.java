package com.flowforge.workflow;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;
import java.util.Set;

public final class WorkflowDtos {

    private WorkflowDtos() {}

    public record CreateWorkflowRequest(
            @NotBlank @Size(max = 140) String name,
            @Size(max = 800) String description,
            @NotEmpty List<@Valid TaskRequest> tasks
    ) {}

    public record UpdateScheduleRequest(
            @NotNull ScheduleType scheduleType,
            @Size(max = 120) String cronExpression,
            @Size(max = 64) String scheduleTimezone
    ) {}

    public record TaskRequest(
            @NotBlank @Size(max = 120) String taskKey,
            @NotNull TaskType taskType,
            Set<@NotBlank String> dependsOn
    ) {}

    public record TaskResponse(
            Long id,
            String taskKey,
            TaskType taskType,
            Set<String> dependsOn
    ) {
        static TaskResponse from(TaskDefinition task) {
            return new TaskResponse(task.getId(), task.getTaskKey(), task.getTaskType(), task.getDependsOn());
        }
    }

    public record WorkflowResponse(
            Long id,
            String name,
            String description,
            WorkflowStatus status,
            ScheduleType scheduleType,
            String cronExpression,
            String scheduleTimezone,
            Long version,
            List<TaskResponse> tasks,
            Instant createdAt,
            Instant updatedAt
    ) {
        static WorkflowResponse from(WorkflowDefinition workflow) {
            return new WorkflowResponse(
                    workflow.getId(),
                    workflow.getName(),
                    workflow.getDescription(),
                    workflow.getStatus(),
                    workflow.getScheduleType(),
                    workflow.getCronExpression(),
                    workflow.getScheduleTimezone(),
                    workflow.getVersion(),
                    workflow.getTasks().stream().map(TaskResponse::from).toList(),
                    workflow.getCreatedAt(),
                    workflow.getUpdatedAt()
            );
        }
    }
}
