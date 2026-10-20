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
        indexes = {@Index(name = "idx_execution_dispatch_outbox_pending", columnList = "published_at,id"),
                @Index(name = "idx_execution_dispatch_outbox_claimable", columnList = "published_at,next_publish_at,claimed_until,id")},
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

    @Column(name = "claim_token")
    private java.util.UUID claimToken;

    @Column(name = "claimed_until")
    private Instant claimedUntil;

    @Column(name = "next_publish_at")
    private Instant nextPublishAt;

    @Column(name = "publish_attempts", nullable = false)
    private int publishAttempts;

    @Column(name = "last_error", length = 120)
    private String lastError;

    public ExecutionDispatchOutbox(WorkflowExecution execution, int attemptNumber, Instant createdAt) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be positive");
        }
        this.execution = Objects.requireNonNull(execution, "execution");
        this.attemptNumber = attemptNumber;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }
}
