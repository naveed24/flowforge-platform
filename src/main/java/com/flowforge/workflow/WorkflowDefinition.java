package com.flowforge.workflow;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "workflows")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WorkflowDefinition {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 140)
    private String name;

    @Column(length = 800)
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private WorkflowStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false, length = 20)
    private ScheduleType scheduleType;

    @Column(name = "cron_expression", length = 120)
    private String cronExpression;

    @Column(name = "schedule_timezone", nullable = false, length = 64)
    private String scheduleTimezone;

    @Version
    private Long version;

    @OneToMany(mappedBy = "workflow", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("id ASC")
    @Builder.Default
    private List<TaskDefinition> tasks = new ArrayList<>();

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public void addTask(TaskDefinition task) {
        tasks.add(task);
        task.setWorkflow(this);
    }

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
        if (status == null) status = WorkflowStatus.DRAFT;
        if (scheduleType == null) scheduleType = ScheduleType.MANUAL;
        if (scheduleTimezone == null || scheduleTimezone.isBlank()) scheduleTimezone = "UTC";
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
