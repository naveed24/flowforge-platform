package com.flowforge.workflow;

import org.springframework.scheduling.support.CronExpression;

import java.time.ZoneId;
import java.time.zone.ZoneRulesException;

public final class WorkflowScheduleValidator {

    private WorkflowScheduleValidator() {}

    public static void validate(ScheduleType type, String cronExpression, String timezone) {
        String zone = timezone == null || timezone.isBlank() ? "UTC" : timezone.trim();
        try {
            ZoneId.of(zone);
        } catch (ZoneRulesException ex) {
            throw new IllegalArgumentException("Invalid schedule timezone: " + zone);
        }

        if (type == ScheduleType.CRON) {
            if (cronExpression == null || cronExpression.isBlank()
                    || !CronExpression.isValidExpression(cronExpression.trim())) {
                throw new IllegalArgumentException("CRON workflows require a valid cron expression");
            }
        } else if (cronExpression != null && !cronExpression.isBlank()) {
            throw new IllegalArgumentException("Manual workflows cannot define a cron expression");
        }
    }
}
