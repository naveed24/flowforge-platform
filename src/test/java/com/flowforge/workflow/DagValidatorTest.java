package com.flowforge.workflow;

import com.flowforge.common.BadRequestException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

class DagValidatorTest {

    private final DagValidator validator = new DagValidator();

    @Test
    void acceptsValidDag() {
        var tasks = List.of(
                new WorkflowDtos.TaskRequest("extract", TaskType.HTTP, Set.of()),
                new WorkflowDtos.TaskRequest("transform", TaskType.JAVA, Set.of("extract")),
                new WorkflowDtos.TaskRequest("publish", TaskType.HTTP, Set.of("transform"))
        );

        assertThatCode(() -> validator.validate(tasks))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsCycles() {
        var tasks = List.of(
                new WorkflowDtos.TaskRequest("a", TaskType.JAVA, Set.of("b")),
                new WorkflowDtos.TaskRequest("b", TaskType.JAVA, Set.of("a"))
        );

        assertThatThrownBy(() -> validator.validate(tasks))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void rejectsMissingDependency() {
        var tasks = List.of(
                new WorkflowDtos.TaskRequest("publish", TaskType.HTTP, Set.of("missing"))
        );

        assertThatThrownBy(() -> validator.validate(tasks))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("unknown task");
    }
}
