package com.flowforge.execution;

import com.flowforge.workflow.WorkflowDefinition;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "workflow_executions", indexes = {
        @Index(name = "idx_workflow_executions_workflow_id", columnList = "workflow_id"),
        @Index(name = "idx_workflow_executions_status", columnList = "status")
}, uniqueConstraints = {
        @UniqueConstraint(name = "uq_workflow_executions_idempotency",
                columnNames = {"workflow_id", "idempotency_key"})
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WorkflowExecution {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "workflow_id", nullable = false)
    private WorkflowDefinition workflow;

    @Column(name = "idempotency_key", length = 128)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ExecutionStatus status;

    @Version
    private Long version;

    @Builder.Default
    @Column(name = "attempt_count", nullable = false)
    private int attemptCount = 0;

    @Builder.Default
    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts = RetryBackoffPolicy.DEFAULT_MAX_ATTEMPTS;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @PrePersist
    void onCreate() {
        if (status == null) status = ExecutionStatus.QUEUED;
        if (createdAt == null) createdAt = Instant.now();
    }
}
