package com.flowforge.workflow;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/workflows")
@RequiredArgsConstructor
public class WorkflowController {

    private final WorkflowService workflowService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public WorkflowDtos.WorkflowResponse create(
            @Valid @RequestBody WorkflowDtos.CreateWorkflowRequest request) {
        return workflowService.create(request);
    }

    @GetMapping
    public List<WorkflowDtos.WorkflowResponse> list() {
        return workflowService.list();
    }

    @GetMapping("/{id}")
    public WorkflowDtos.WorkflowResponse get(@PathVariable Long id) {
        return workflowService.get(id);
    }
}
