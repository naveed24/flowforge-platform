package com.flowforge.workflow;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class WorkflowServiceIntegrationTest {

    @Autowired
    WorkflowService workflowService;

    @Test
    void createsWorkflowWithTasks() {
        var request = new WorkflowDtos.CreateWorkflowRequest(
                "daily-report",
                "Build daily report",
                List.of(
                        new WorkflowDtos.TaskRequest("extract", TaskType.HTTP, Set.of()),
                        new WorkflowDtos.TaskRequest("publish", TaskType.HTTP, Set.of("extract"))
                )
        );

        var created = workflowService.create(request);

        assertThat(created.id()).isNotNull();
        assertThat(created.status()).isEqualTo(WorkflowStatus.DRAFT);
        assertThat(created.tasks()).hasSize(2);
    }
}
