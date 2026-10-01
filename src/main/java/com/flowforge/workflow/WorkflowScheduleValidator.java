package com.flowforge.workflow;

import com.flowforge.common.BadRequestException;
import org.springframework.scheduling.support.CronExpression;

import java.time.DateTimeException;
import java.time.ZoneId;

public final class WorkflowScheduleValidator {

    private WorkflowScheduleValidator() {}

    public static void validate(ScheduleType type, String cronExpression, String timezone) {
        ScheduleType effectiveType = type == null ? ScheduleType.MANUAL : type;
        String zone = timezone == null || timezone.isBlank() ? "UTC" : timezone.trim();

        try {
            ZoneId.of(zone);
        } catch (DateTimeException ex) {
            throw new BadRequestException("Invalid schedule timezone: " + zone);
        }

        if (effectiveType == ScheduleType.CRON) {
            if (cronExpression == null || cronExpression.isBlank()
                    || !CronExpression.isValidExpression(cronExpression.trim())) {
                throw new BadRequestException("CRON workflows require a valid cron expression");
            }
        } else if (cronExpression != null && !cronExpression.isBlank()) {
            throw new BadRequestException("Manual workflows cannot define a cron expression");
        }
    }
}
