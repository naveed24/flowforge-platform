package com.flowforge.execution;

import com.flowforge.workflow.*;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
class ExecutionControllerIntegrationTest {

    @Autowired private WebApplicationContext context;
    @Autowired private WorkflowService workflowService;
    @Autowired private ExecutionService executionService;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void submitsLooksUpAndCancelsAnExecution() throws Exception {
        var workflow = activeWorkflow("http-execution-success");

        var submitted = mvc.perform(post("/api/v1/workflows/{id}/executions", workflow.id()))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", startsWith("/api/v1/executions/")))
                .andExpect(jsonPath("$.workflowId").value(workflow.id()))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andReturn();

        String json = submitted.getResponse().getContentAsString();
        long executionId = ((Number) JsonPath.read(json, "$.id")).longValue();
        long version = ((Number) JsonPath.read(json, "$.version")).longValue();

        mvc.perform(get("/api/v1/executions/{id}", executionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(executionId))
                .andExpect(jsonPath("$.status").value("QUEUED"));

        mvc.perform(post("/api/v1/executions/{id}/cancel", executionId)
                        .contentType("application/json")
                        .content("{\"expectedVersion\":" + version + "}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.finishedAt").isNotEmpty());

        mvc.perform(get("/api/v1/executions/{id}", executionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void rejectsStaleCancellationAndPreventsDuplicateCancellation() throws Exception {
        var workflow = activeWorkflow("http-execution-stale");
        var queued = executionService.enqueue(workflow.id());
        var running = executionService.start(queued.getId());

        mvc.perform(post("/api/v1/executions/{id}/cancel", queued.getId())
                        .contentType("application/json")
                        .content("{\"expectedVersion\":" + queued.getVersion() + "}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(
                        "Execution was modified; fetch the latest version and retry"));

        mvc.perform(get("/api/v1/executions/{id}", queued.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RUNNING"));

        mvc.perform(post("/api/v1/executions/{id}/cancel", queued.getId())
                        .contentType("application/json")
                        .content("{\"expectedVersion\":" + running.getVersion() + "}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mvc.perform(post("/api/v1/executions/{id}/cancel", queued.getId())
                        .contentType("application/json")
                        .content("{\"expectedVersion\":" + (running.getVersion() + 1) + "}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void validatesCancellationAndRejectsNonActiveWorkflows() throws Exception {
        var draft = workflowService.create(request("http-execution-draft"));
        mvc.perform(post("/api/v1/workflows/{id}/executions", draft.id()))
                .andExpect(status().isBadRequest());

        var workflow = activeWorkflow("http-execution-validation");
        var queued = executionService.enqueue(workflow.id());

        mvc.perform(post("/api/v1/executions/{id}/cancel", queued.getId())
                        .contentType("application/json")
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.expectedVersion").exists());

        mvc.perform(get("/api/v1/executions/{id}", Long.MAX_VALUE))
                .andExpect(status().isNotFound());

        mvc.perform(get("/api/v1/executions/{id}", queued.getId()))
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    private WorkflowDtos.WorkflowResponse activeWorkflow(String name) {
        var created = workflowService.create(request(name));
        return workflowService.activate(created.id(), created.version());
    }

    private WorkflowDtos.CreateWorkflowRequest request(String name) {
        return new WorkflowDtos.CreateWorkflowRequest(
                name,
                "REST execution integration test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))
        );
    }
}
