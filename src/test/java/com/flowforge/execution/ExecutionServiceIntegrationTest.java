package com.flowforge.execution;

import com.flowforge.common.BadRequestException;
import com.flowforge.workflow.*;
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
class ExecutionServiceIntegrationTest {

    @Autowired ExecutionService executionService;
    @Autowired WorkflowService workflowService;

    @Test
    void persistsSuccessfulExecutionLifecycle() {
        var workflow = activeWorkflow("execution-success");

        var queued = executionService.enqueue(workflow.id());
        assertThat(queued.getStatus()).isEqualTo(ExecutionStatus.QUEUED);
        assertThat(queued.getCreatedAt()).isNotNull();

        var running = executionService.start(queued.getId());
        assertThat(running.getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(running.getStartedAt()).isNotNull();

        var succeeded = executionService.succeed(queued.getId());
        assertThat(succeeded.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(succeeded.getFinishedAt()).isNotNull();
    }

    @Test
    void rejectsInvalidExecutionTransitions() {
        var workflow = activeWorkflow("execution-invalid-transition");
        var queued = executionService.enqueue(workflow.id());

        assertThatThrownBy(() -> executionService.succeed(queued.getId()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("RUNNING");

        executionService.start(queued.getId());
        executionService.fail(queued.getId());

        assertThatThrownBy(() -> executionService.start(queued.getId()))
                .isInstanceOf(BadRequestException.class);
        assertThatThrownBy(() -> executionService.cancel(queued.getId(), executionService.get(queued.getId()).getVersion()))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void onlyActiveWorkflowsCanBeEnqueued() {
        var draft = workflowService.create(request("execution-draft"));

        assertThatThrownBy(() -> executionService.enqueue(draft.id()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("active workflows");
    }

    private WorkflowDtos.WorkflowResponse activeWorkflow(String name) {
        var created = workflowService.create(request(name));
        return workflowService.activate(created.id(), created.version());
    }

    private WorkflowDtos.CreateWorkflowRequest request(String name) {
        return new WorkflowDtos.CreateWorkflowRequest(
                name,
                "Execution state machine test",
                List.of(new WorkflowDtos.TaskRequest("run", TaskType.HTTP, Set.of()))
        );
    }
}
