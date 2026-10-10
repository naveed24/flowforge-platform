package com.flowforge.workflow;

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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
class WorkflowLifecycleApiIntegrationTest {
    @Autowired private WebApplicationContext context;
    @Autowired private WorkflowService workflows;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void persistsCronScheduleAndExercisesLifecycleOverRest() throws Exception {
        var created = workflows.create(new WorkflowDtos.CreateWorkflowRequest(
                "http-cron-lifecycle-coverage", "HTTP workflow lifecycle",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))));

        var scheduled = mvc.perform(put("/api/v1/workflows/{id}/schedule", created.id())
                        .contentType("application/json")
                        .content(schedule(created.version(), "CRON", "0 0 6 * * *", "Asia/Kolkata")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scheduleType").value("CRON"))
                .andExpect(jsonPath("$.cronExpression").value("0 0 6 * * *"))
                .andExpect(jsonPath("$.scheduleTimezone").value("Asia/Kolkata"))
                .andReturn();
        var activeVersion = workflows.get(created.id()).version();

        mvc.perform(put("/api/v1/workflows/{id}/schedule", created.id())
                        .contentType("application/json")
                        .content(schedule(created.version(), "MANUAL", null, "UTC")))
                .andExpect(status().isConflict());
        mvc.perform(put("/api/v1/workflows/{id}/schedule", created.id())
                        .contentType("application/json")
                        .content(schedule(activeVersion, "CRON", "invalid", "UTC")))
                .andExpect(status().isBadRequest());

        mvc.perform(post("/api/v1/workflows/{id}/activate", created.id())
                        .contentType("application/json")
                        .content(version(activeVersion)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACTIVE"));
        mvc.perform(post("/api/v1/workflows/{id}/pause", created.id())
                        .contentType("application/json")
                        .content(version(activeVersion)))
                .andExpect(status().isConflict());

        long newVersion = workflows.get(created.id()).version();
        mvc.perform(post("/api/v1/workflows/{id}/pause", created.id())
                        .contentType("application/json").content(version(newVersion)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PAUSED"));
        mvc.perform(post("/api/v1/workflows/{id}/archive", created.id())
                        .contentType("application/json")
                        .content(version(workflows.get(created.id()).version())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ARCHIVED"));
    }

    private String version(long expectedVersion) {
        return "{\"expectedVersion\":" + expectedVersion + "}";
    }

    private String schedule(long version, String type, String cron, String zone) {
        return "{\"expectedVersion\":" + version + ",\"scheduleType\":\"" + type
                + "\",\"cronExpression\":" + (cron == null ? "null" : "\"" + cron + "\"")
                + ",\"scheduleTimezone\":\"" + zone + "\"}";
    }
}
