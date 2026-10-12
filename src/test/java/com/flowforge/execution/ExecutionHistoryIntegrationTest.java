package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import com.flowforge.workflow.WorkflowDefinition;
import com.flowforge.workflow.WorkflowDtos;
import com.flowforge.workflow.WorkflowRepository;
import com.flowforge.workflow.WorkflowService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
class ExecutionHistoryIntegrationTest {

    @Autowired private WebApplicationContext context;
    @Autowired private WorkflowService workflowService;
    @Autowired private WorkflowRepository workflowRepository;
    @Autowired private WorkflowExecutionRepository executionRepository;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void paginatesNewestFirstWithDeterministicTiesAndWorkflowIsolation() throws Exception {
        var firstWorkflow = workflow("history-primary");
        var otherWorkflow = workflow("history-other");
        Instant sameTime = Instant.parse("2026-10-12T00:00:00Z");

        var oldest = persist(firstWorkflow, ExecutionStatus.QUEUED, sameTime);
        var middle = persist(firstWorkflow, ExecutionStatus.SUCCEEDED, sameTime);
        var newest = persist(firstWorkflow, ExecutionStatus.FAILED, sameTime);
        persist(otherWorkflow, ExecutionStatus.FAILED, sameTime);

        mvc.perform(get("/api/v1/workflows/{id}/executions", firstWorkflow.getId())
                        .param("page", "0").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(2))
                .andExpect(jsonPath("$.items[0].id").value(newest.getId()))
                .andExpect(jsonPath("$.items[1].id").value(middle.getId()))
                .andExpect(jsonPath("$.items[0].workflowId").value(firstWorkflow.getId()))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.hasNext").value(true));

        mvc.perform(get("/api/v1/workflows/{id}/executions", firstWorkflow.getId())
                        .param("page", "1").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].id").value(oldest.getId()))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.hasNext").value(false));

        mvc.perform(get("/api/v1/workflows/{id}/executions", firstWorkflow.getId())
                        .param("page", "2").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items").isEmpty())
                .andExpect(jsonPath("$.totalElements").value(3));
    }

    @Test
    void filtersByStatusWithoutLeakingExecutionsFromOtherWorkflows() throws Exception {
        var first = workflow("history-filter-primary");
        var other = workflow("history-filter-other");
        Instant time = Instant.parse("2026-10-12T01:00:00Z");
        var failed = persist(first, ExecutionStatus.FAILED, time);
        persist(first, ExecutionStatus.QUEUED, time);
        persist(other, ExecutionStatus.FAILED, time);

        mvc.perform(get("/api/v1/workflows/{id}/executions", first.getId())
                        .param("status", "FAILED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].id").value(failed.getId()))
                .andExpect(jsonPath("$.items[0].status").value("FAILED"))
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.size").value(20));
    }

    @Test
    void validatesPaginationAndReturnsNotFoundForUnknownWorkflow() throws Exception {
        var existing = workflow("history-validation");

        mvc.perform(get("/api/v1/workflows/{id}/executions", existing.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items").isEmpty())
                .andExpect(jsonPath("$.totalElements").value(0));

        for (String size : List.of("0", "-1", "101")) {
            mvc.perform(get("/api/v1/workflows/{id}/executions", existing.getId())
                            .param("size", size))
                    .andExpect(status().isBadRequest());
        }
        mvc.perform(get("/api/v1/workflows/{id}/executions", existing.getId())
                        .param("page", "-1"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/workflows/{id}/executions", existing.getId())
                        .param("status", "NOT_A_STATUS"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/workflows/{id}/executions", Long.MAX_VALUE))
                .andExpect(status().isNotFound());
    }

    private WorkflowDefinition workflow(String name) {
        var created = workflowService.create(new WorkflowDtos.CreateWorkflowRequest(
                name, "Execution history test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));
        return workflowRepository.findById(created.id()).orElseThrow();
    }

    private WorkflowExecution persist(WorkflowDefinition workflow, ExecutionStatus status, Instant createdAt) {
        var builder = WorkflowExecution.builder()
                .workflow(workflow).status(status).createdAt(createdAt);
        if (status == ExecutionStatus.SUCCEEDED || status == ExecutionStatus.FAILED) {
            builder.attemptCount(1).startedAt(createdAt).finishedAt(createdAt);
        }
        return executionRepository.saveAndFlush(builder.build());
    }
}
