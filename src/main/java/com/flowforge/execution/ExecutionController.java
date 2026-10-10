package com.flowforge.execution;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class ExecutionController {

    private final ExecutionService executionService;
    private final ExecutionSubmissionService submissionService;

    @PostMapping("/workflows/{workflowId}/executions")
    public ResponseEntity<ExecutionDtos.ExecutionResponse> enqueue(
            @PathVariable Long workflowId,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {
        var submission = submissionService.submit(workflowId, idempotencyKey);
        var execution = ExecutionDtos.ExecutionResponse.from(submission.execution());
        URI location = URI.create("/api/v1/executions/" + execution.id());
        return ResponseEntity.status(submission.replayed() ? HttpStatus.OK : HttpStatus.CREATED)
                .location(location).body(execution);
    }

    @GetMapping("/executions/{executionId}")
    public ExecutionDtos.ExecutionResponse get(@PathVariable Long executionId) {
        return ExecutionDtos.ExecutionResponse.from(executionService.get(executionId));
    }

    @PostMapping("/executions/{executionId}/cancel")
    public ExecutionDtos.ExecutionResponse cancel(
            @PathVariable Long executionId,
            @Valid @RequestBody ExecutionDtos.CancelExecutionRequest request) {
        return ExecutionDtos.ExecutionResponse.from(
                executionService.cancel(executionId, request.expectedVersion()));
    }
}
