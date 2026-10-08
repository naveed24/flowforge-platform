package com.flowforge.execution;

import com.flowforge.common.BadRequestException;
import com.flowforge.common.ConflictException;
import com.flowforge.common.NotFoundException;
import com.flowforge.workflow.WorkflowDefinition;
import com.flowforge.workflow.WorkflowRepository;
import com.flowforge.workflow.WorkflowStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ExecutionService {

    private final WorkflowExecutionRepository executionRepository;
    private final WorkflowRepository workflowRepository;

    @Transactional
    public WorkflowExecution enqueue(Long workflowId) {
        WorkflowDefinition workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new NotFoundException("Workflow not found: " + workflowId));
        if (workflow.getStatus() != WorkflowStatus.ACTIVE) {
            throw new BadRequestException("Only active workflows can be executed");
        }
        return executionRepository.saveAndFlush(WorkflowExecution.builder()
                .workflow(workflow)
                .status(ExecutionStatus.QUEUED)
                .build());
    }

    @Transactional
    public WorkflowExecution start(Long executionId) {
        WorkflowExecution execution = find(executionId);
        requireStatus(execution, ExecutionStatus.QUEUED, "started");
        execution.setStatus(ExecutionStatus.RUNNING);
        execution.setStartedAt(Instant.now());
        return executionRepository.saveAndFlush(execution);
    }

    @Transactional
    public WorkflowExecution succeed(Long executionId) {
        return finish(executionId, ExecutionStatus.SUCCEEDED);
    }

    @Transactional
    public WorkflowExecution fail(Long executionId) {
        return finish(executionId, ExecutionStatus.FAILED);
    }

    @Transactional
    public WorkflowExecution cancel(Long executionId, Long expectedVersion) {
        WorkflowExecution execution = find(executionId);
        if (!execution.getVersion().equals(expectedVersion)) {
            throw new ConflictException("Execution was modified; fetch the latest version and retry");
        }
        if (execution.getStatus() != ExecutionStatus.QUEUED && execution.getStatus() != ExecutionStatus.RUNNING) {
            throw new BadRequestException("Only queued or running executions can be cancelled");
        }
        execution.setStatus(ExecutionStatus.CANCELLED);
        execution.setFinishedAt(Instant.now());
        return executionRepository.saveAndFlush(execution);
    }

    public WorkflowExecution get(Long executionId) {
        return find(executionId);
    }

    private WorkflowExecution finish(Long executionId, ExecutionStatus target) {
        WorkflowExecution execution = find(executionId);
        requireStatus(execution, ExecutionStatus.RUNNING, "finished");
        execution.setStatus(target);
        execution.setFinishedAt(Instant.now());
        return executionRepository.saveAndFlush(execution);
    }

    private WorkflowExecution find(Long executionId) {
        return executionRepository.findById(executionId)
                .orElseThrow(() -> new NotFoundException("Execution not found: " + executionId));
    }

    private void requireStatus(WorkflowExecution execution, ExecutionStatus expected, String action) {
        if (execution.getStatus() != expected) {
            throw new BadRequestException("Execution must be " + expected + " before it can be " + action);
        }
    }
}
