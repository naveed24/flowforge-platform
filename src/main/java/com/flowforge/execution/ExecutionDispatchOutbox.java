package com.flowforge.execution;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Objects;

/**
 * Durable intent to dispatch the next attempt of an execution. Publication is
 * a separate milestone; rows remain pending until an acknowledged publish.
 */
@Entity
@Table(name = "execution_dispatch_outbox",
        indexes = @Index(name = "idx_execution_dispatch_outbox_pending",
                columnList = "published_at,id"),
        uniqueConstraints = @UniqueConstraint(name = "uq_execution_dispatch_outbox_attempt",
                columnNames = {"execution_id", "attempt_number"}))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ExecutionDispatchOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "execution_id", nullable = false)
    private WorkflowExecution execution;

    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    public ExecutionDispatchOutbox(WorkflowExecution execution, int attemptNumber, Instant createdAt) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be positive");
        }
        this.execution = Objects.requireNonNull(execution, "execution");
        this.attemptNumber = attemptNumber;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }
}
