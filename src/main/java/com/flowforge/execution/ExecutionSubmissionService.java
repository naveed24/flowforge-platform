package com.flowforge.execution;

import com.flowforge.common.BadRequestException;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/** Routes HTTP submissions through DB-backed, per-workflow idempotency. */
@Service
@RequiredArgsConstructor
public class ExecutionSubmissionService {

    private final ExecutionService executionService;
    private final WorkflowExecutionRepository repository;

    public record Submission(WorkflowExecution execution, boolean replayed) {}

    public Submission submit(Long workflowId, String key) {
        if (key == null) {
            return new Submission(executionService.enqueue(workflowId), false);
        }
        if (!key.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")) {
            throw new BadRequestException("Idempotency-Key must be 1-128 ASCII token characters");
        }
        var previous = repository.findIdempotentSubmission(workflowId, key);
        if (previous.isPresent()) {
            return new Submission(previous.get(), true);
        }
        try {
            // Transactional service call commits (or rolls back) before returning here.
            return new Submission(executionService.enqueue(workflowId, key), false);
        } catch (DataIntegrityViolationException databaseError) {
            // A concurrent insert may have won. Only recover if its scoped key exists.
            return repository.findIdempotentSubmission(workflowId, key)
                    .map(found -> new Submission(found, true))
                    .orElseThrow(() -> databaseError);
        }
    }
}
