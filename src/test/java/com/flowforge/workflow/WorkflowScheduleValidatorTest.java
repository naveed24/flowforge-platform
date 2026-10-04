package com.flowforge.workflow;

import com.flowforge.common.BadRequestException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowScheduleValidatorTest {

    @Test
    void acceptsValidCronWithTimezone() {
        WorkflowScheduleValidator.validate(ScheduleType.CRON, "0 0 6 * * *", "Asia/Kolkata");
    }

    @Test
    void rejectsCronWithoutExpression() {
        assertThatThrownBy(() -> WorkflowScheduleValidator.validate(ScheduleType.CRON, null, "UTC"))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("valid cron");
    }

    @Test
    void rejectsInvalidCronExpression() {
        assertThatThrownBy(() -> WorkflowScheduleValidator.validate(ScheduleType.CRON, "invalid", "UTC"))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void rejectsManualScheduleWithCronExpression() {
        assertThatThrownBy(() -> WorkflowScheduleValidator.validate(ScheduleType.MANUAL, "0 0 6 * * *", "UTC"))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Manual workflows");
    }

    @Test
    void rejectsUnknownTimezone() {
        assertThatThrownBy(() -> WorkflowScheduleValidator.validate(ScheduleType.CRON, "0 0 6 * * *", "Not/A_Timezone"))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Invalid schedule timezone");
    }

    @Test
    void defaultsBlankTimezoneToUtc() {
        WorkflowScheduleValidator.validate(ScheduleType.MANUAL, null, " ");
    }
}
