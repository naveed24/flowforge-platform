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

import java.time.Clock;
import java.time.Instant;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ExecutionService {

    private final WorkflowExecutionRepository executionRepository;
    private final WorkflowRepository workflowRepository;
    private final Clock clock;

    @Transactional
    public WorkflowExecution enqueue(Long workflowId) {
        return enqueue(workflowId, null);
    }

    @Transactional
    public WorkflowExecution enqueue(Long workflowId, String idempotencyKey) {
        WorkflowDefinition workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new NotFoundException("Workflow not found: " + workflowId));
        if (workflow.getStatus() != WorkflowStatus.ACTIVE) {
            throw new BadRequestException("Only active workflows can be executed");
        }
        return executionRepository.saveAndFlush(WorkflowExecution.builder()
                .workflow(workflow)
                .idempotencyKey(idempotencyKey)
                .status(ExecutionStatus.QUEUED)
                .attemptCount(0)
                .maxAttempts(RetryBackoffPolicy.DEFAULT_MAX_ATTEMPTS)
                .build());
    }

    @Transactional
    public WorkflowExecution start(Long executionId) {
        WorkflowExecution execution = find(executionId);
        Instant now = Instant.now(clock);
        if (execution.getStatus() == ExecutionStatus.RETRY_WAIT) {
            if (execution.getNextAttemptAt() == null || now.isBefore(execution.getNextAttemptAt())) {
                throw new BadRequestException("Retry is not due yet");
            }
        } else {
            requireStatus(execution, ExecutionStatus.QUEUED, "started");
        }
        execution.setStatus(ExecutionStatus.RUNNING);
        execution.setAttemptCount(execution.getAttemptCount() + 1);
        execution.setStartedAt(now);
        execution.setNextAttemptAt(null);
        return executionRepository.saveAndFlush(execution);
    }

    @Transactional
    public WorkflowExecution succeed(Long executionId) {
        return finish(executionId, ExecutionStatus.SUCCEEDED);
    }

    @Transactional
    public WorkflowExecution fail(Long executionId) {
        WorkflowExecution execution = find(executionId);
        requireStatus(execution, ExecutionStatus.RUNNING, "finished");
        Instant now = Instant.now(clock);
        if (execution.getAttemptCount() < execution.getMaxAttempts()) {
            execution.setStatus(ExecutionStatus.RETRY_WAIT);
            execution.setNextAttemptAt(now.plus(RetryBackoffPolicy.delayAfterFailure(execution.getAttemptCount())));
        } else {
            execution.setStatus(ExecutionStatus.FAILED);
            execution.setNextAttemptAt(null);
            execution.setFinishedAt(now);
        }
        return executionRepository.saveAndFlush(execution);
    }

    @Transactional
    public WorkflowExecution cancel(Long executionId, Long expectedVersion) {
        WorkflowExecution execution = find(executionId);
        if (!execution.getVersion().equals(expectedVersion)) {
            throw new ConflictException("Execution was modified; fetch the latest version and retry");
        }
        if (execution.getStatus() != ExecutionStatus.QUEUED && execution.getStatus() != ExecutionStatus.RUNNING
                && execution.getStatus() != ExecutionStatus.RETRY_WAIT) {
            throw new BadRequestException("Only queued, running or retry-waiting executions can be cancelled");
        }
        execution.setStatus(ExecutionStatus.CANCELLED);
        execution.setNextAttemptAt(null);
        execution.setFinishedAt(Instant.now(clock));
        return executionRepository.saveAndFlush(execution);
    }

    public WorkflowExecution get(Long executionId) {
        return find(executionId);
    }

    private WorkflowExecution finish(Long executionId, ExecutionStatus target) {
        WorkflowExecution execution = find(executionId);
        requireStatus(execution, ExecutionStatus.RUNNING, "finished");
        execution.setStatus(target);
        execution.setFinishedAt(Instant.now(clock));
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
