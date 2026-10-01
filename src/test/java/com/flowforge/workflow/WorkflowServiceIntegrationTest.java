package com.flowforge.workflow;

import com.flowforge.common.BadRequestException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class WorkflowServiceIntegrationTest {

    @Autowired
    WorkflowService workflowService;

    @Test
    void createsWorkflowWithManualScheduleDefaults() {
        var request = createRequest("daily-report-defaults");

        var created = workflowService.create(request);

        assertThat(created.id()).isNotNull();
        assertThat(created.status()).isEqualTo(WorkflowStatus.DRAFT);
        assertThat(created.scheduleType()).isEqualTo(ScheduleType.MANUAL);
        assertThat(created.scheduleTimezone()).isEqualTo("UTC");
        assertThat(created.cronExpression()).isNull();
        assertThat(created.tasks()).hasSize(2);
    }

    @Test
    void configuresCronScheduleAndTransitionsLifecycle() {
        var created = workflowService.create(createRequest("daily-report-lifecycle"));

        var scheduled = workflowService.updateSchedule(
                created.id(),
                new WorkflowDtos.UpdateScheduleRequest(
                        ScheduleType.CRON,
                        "0 0 6 * * *",
                        "Asia/Kolkata"
                )
        );

        assertThat(scheduled.scheduleType()).isEqualTo(ScheduleType.CRON);
        assertThat(scheduled.cronExpression()).isEqualTo("0 0 6 * * *");
        assertThat(scheduled.scheduleTimezone()).isEqualTo("Asia/Kolkata");

        var active = workflowService.activate(created.id());
        assertThat(active.status()).isEqualTo(WorkflowStatus.ACTIVE);

        var paused = workflowService.pause(created.id());
        assertThat(paused.status()).isEqualTo(WorkflowStatus.PAUSED);
    }

    @Test
    void rejectsInvalidCronSchedule() {
        var created = workflowService.create(createRequest("invalid-cron-workflow"));

        assertThatThrownBy(() -> workflowService.updateSchedule(
                created.id(),
                new WorkflowDtos.UpdateScheduleRequest(
                        ScheduleType.CRON,
                        "not-a-cron",
                        "UTC"
                )
        ))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("valid cron");
    }

    private WorkflowDtos.CreateWorkflowRequest createRequest(String name) {
        return new WorkflowDtos.CreateWorkflowRequest(
                name,
                "Build daily report",
                List.of(
                        new WorkflowDtos.TaskRequest("extract", TaskType.HTTP, Set.of()),
                        new WorkflowDtos.TaskRequest("publish", TaskType.HTTP, Set.of("extract"))
                )
        );
    }
}
