package com.flowforge.workflow;

import com.flowforge.common.BadRequestException;
import com.flowforge.common.ConflictException;
import com.flowforge.common.NotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashSet;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class WorkflowService {

    private final WorkflowRepository repository;
    private final DagValidator dagValidator;

    @Transactional
    public WorkflowDtos.WorkflowResponse create(WorkflowDtos.CreateWorkflowRequest request) {
        String name = request.name().trim();

        if (repository.existsByNameIgnoreCase(name)) {
            throw new ConflictException("Workflow name already exists: " + name);
        }

        dagValidator.validate(request.tasks());

        WorkflowDefinition workflow = WorkflowDefinition.builder()
                .name(name)
                .description(request.description())
                .status(WorkflowStatus.DRAFT)
                .scheduleType(ScheduleType.MANUAL)
                .scheduleTimezone("UTC")
                .build();

        for (WorkflowDtos.TaskRequest taskRequest : request.tasks()) {
            TaskDefinition task = TaskDefinition.builder()
                    .taskKey(taskRequest.taskKey().trim())
                    .taskType(taskRequest.taskType())
                    .dependsOn(taskRequest.dependsOn() == null
                            ? new LinkedHashSet<>()
                            : new LinkedHashSet<>(taskRequest.dependsOn()))
                    .build();

            workflow.addTask(task);
        }

        return WorkflowDtos.WorkflowResponse.from(repository.saveAndFlush(workflow));
    }

    public List<WorkflowDtos.WorkflowResponse> list() {
        return repository.findAll().stream()
                .map(WorkflowDtos.WorkflowResponse::from)
                .toList();
    }

    public WorkflowDtos.WorkflowResponse get(Long id) {
        return WorkflowDtos.WorkflowResponse.from(findWorkflow(id));
    }

    @Transactional
    public WorkflowDtos.WorkflowResponse updateSchedule(
            Long id,
            WorkflowDtos.UpdateScheduleRequest request) {

        WorkflowDefinition workflow = findWorkflow(id);
        if (!workflow.getVersion().equals(request.expectedVersion())) {
            throw new ConflictException("Workflow was modified; fetch the latest version and retry");
        }
        WorkflowScheduleValidator.validate(
                request.scheduleType(),
                request.cronExpression(),
                request.scheduleTimezone()
        );

        workflow.setScheduleType(request.scheduleType());
        workflow.setCronExpression(normalize(request.cronExpression()));
        workflow.setScheduleTimezone(
                request.scheduleTimezone() == null || request.scheduleTimezone().isBlank()
                        ? "UTC"
                        : request.scheduleTimezone().trim()
        );

        return WorkflowDtos.WorkflowResponse.from(repository.saveAndFlush(workflow));
    }

    @Transactional
    public WorkflowDtos.WorkflowResponse activate(Long id) {
        WorkflowDefinition workflow = findWorkflow(id);

        if (workflow.getStatus() == WorkflowStatus.ARCHIVED) {
            throw new BadRequestException("Archived workflows cannot be activated");
        }

        WorkflowScheduleValidator.validate(
                workflow.getScheduleType(),
                workflow.getCronExpression(),
                workflow.getScheduleTimezone()
        );

        workflow.setStatus(WorkflowStatus.ACTIVE);
        return WorkflowDtos.WorkflowResponse.from(repository.saveAndFlush(workflow));
    }

    @Transactional
    public WorkflowDtos.WorkflowResponse pause(Long id) {
        WorkflowDefinition workflow = findWorkflow(id);

        if (workflow.getStatus() != WorkflowStatus.ACTIVE) {
            throw new BadRequestException("Only active workflows can be paused");
        }

        workflow.setStatus(WorkflowStatus.PAUSED);
        return WorkflowDtos.WorkflowResponse.from(repository.saveAndFlush(workflow));
    }

    private WorkflowDefinition findWorkflow(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Workflow not found: " + id));
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
