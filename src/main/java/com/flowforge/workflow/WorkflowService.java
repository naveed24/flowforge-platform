package com.flowforge.workflow;

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

        return WorkflowDtos.WorkflowResponse.from(repository.save(workflow));
    }

    public List<WorkflowDtos.WorkflowResponse> list() {
        return repository.findAll().stream()
                .map(WorkflowDtos.WorkflowResponse::from)
                .toList();
    }

    public WorkflowDtos.WorkflowResponse get(Long id) {
        return WorkflowDtos.WorkflowResponse.from(
                repository.findById(id)
                        .orElseThrow(() -> new NotFoundException("Workflow not found: " + id))
        );
    }
}
