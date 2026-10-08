package com.flowforge.execution;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class ExecutionController {

    private final ExecutionService executionService;

    @PostMapping("/workflows/{workflowId}/executions")
    public ResponseEntity<ExecutionDtos.ExecutionResponse> enqueue(@PathVariable Long workflowId) {
        var execution = ExecutionDtos.ExecutionResponse.from(executionService.enqueue(workflowId));
        return ResponseEntity.created(URI.create("/api/v1/executions/" + execution.id()))
                .body(execution);
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
