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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
class ExecutionIdempotencyIntegrationTest {

    @Autowired WebApplicationContext context;
    @Autowired WorkflowService workflowService;
    @Autowired ExecutionService executionService;
    @Autowired WorkflowExecutionRepository repository;
    private MockMvc mvc;

    @BeforeEach
    void init() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void sameKeyReturnsOriginalExecutionAndItsLatestStatus() throws Exception {
        var workflow = active("idempotent-retry");
        long count = repository.count();
        var created = mvc.perform(post("/api/v1/workflows/{id}/executions", workflow.id())
                        .header("Idempotency-Key", "order-123:abc"))
                .andExpect(status().isCreated())
                .andReturn();
        long id = getId(created.getResponse().getContentAsString());
        executionService.start(id);
        executionService.succeed(id);

        var replay = mvc.perform(post("/api/v1/workflows/{id}/executions", workflow.id())
                        .header("Idempotency-Key", "order-123:abc"))
                .andExpect(status().isOk())
                .andExpect(header().string("Location", "/api/v1/executions/" + id))
                .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                .andReturn();
        assertThat(getId(replay.getResponse().getContentAsString())).isEqualTo(id);
        assertThat(repository.count()).isEqualTo(count + 1);
    }

    @Test
    void differentKeysAndMissingKeysAlwaysCreateNewExecutions() throws Exception {
        var workflow = active("idempotent-new-keys");
        long first = submit(workflow.id(), "first");
        long second = submit(workflow.id(), "second");
        long third = submit(workflow.id(), null);
        long fourth = submit(workflow.id(), null);
        assertThat(Set.of(first, second, third, fourth)).hasSize(4);
    }

    @Test
    void keysAreScopedToWorkflow() throws Exception {
        var first = active("idempotent-scope-a");
        var second = active("idempotent-scope-b");
        assertThat(submit(first.id(), "shared")).isNotEqualTo(submit(second.id(), "shared"));
    }

    @Test
    void rejectsInvalidKeysWithoutCreatingExecutions() throws Exception {
        var workflow = active("idempotent-bad-key");
        long count = repository.count();
        for (String invalid : List.of("", " ", "key with spaces", "x".repeat(129))) {
            mvc.perform(post("/api/v1/workflows/{id}/executions", workflow.id())
                        .header("Idempotency-Key", invalid))
                    .andExpect(status().isBadRequest());
        }
        assertThat(repository.count()).isEqualTo(count);
    }

    @Test
    void pausedWorkflowAllowsReplaysButNotNewSubmissions() throws Exception {
        var workflow = active("idempotent-after-pause");
        long original = submit(workflow.id(), "original");
        var current = workflowService.get(workflow.id());
        workflowService.pause(current.id(), current.version());

        mvc.perform(post("/api/v1/workflows/{id}/executions", workflow.id())
                        .header("Idempotency-Key", "original"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(original));
        mvc.perform(post("/api/v1/workflows/{id}/executions", workflow.id())
                        .header("Idempotency-Key", "new"))
                .andExpect(status().isBadRequest());
    }

    private long submit(Long workflowId, String key) throws Exception {
        var request = post("/api/v1/workflows/{id}/executions", workflowId);
        if (key != null) request.header("Idempotency-Key", key);
        var response = mvc.perform(request).andExpect(status().isCreated()).andReturn();
        return getId(response.getResponse().getContentAsString());
    }

    private long getId(String json) {
        return ((Number) JsonPath.read(json, "$.id")).longValue();
    }

    private WorkflowDtos.WorkflowResponse active(String name) {
        var created = workflowService.create(new WorkflowDtos.CreateWorkflowRequest(
                name, "Submission idempotency test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));
        return workflowService.activate(created.id(), created.version());
    }
}
