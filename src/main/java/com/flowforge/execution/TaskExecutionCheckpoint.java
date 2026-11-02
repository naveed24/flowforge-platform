package com.flowforge.execution;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A task's durable admission record, shared by all retries of its execution.
 * IN_PROGRESS deliberately survives an uncertain worker crash: an external
 * side effect must be reconciled before another attempt may be admitted.
 */
@Entity
@Table(name = "execution_task_checkpoints",
        uniqueConstraints = @UniqueConstraint(name = "uq_execution_task_checkpoints_task",
                columnNames = {"execution_id", "task_key"}),
        indexes = @Index(name = "idx_execution_task_checkpoints_state",
                columnList = "state,updated_at"))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TaskExecutionCheckpoint {
    public enum State { IN_PROGRESS, RETRYABLE, SUCCEEDED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "execution_id", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private WorkflowExecution execution;

    @Column(name = "task_key", nullable = false, length = 120)
    private String taskKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private State state;

    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    @Column(name = "claim_token", nullable = false)
    private UUID claimToken;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    TaskExecutionCheckpoint(WorkflowExecution execution, String taskKey,
                            int attemptNumber, UUID token, Instant now) {
        this.execution = Objects.requireNonNull(execution);
        this.taskKey = Objects.requireNonNull(taskKey);
        this.attemptNumber = attemptNumber;
        this.claimToken = Objects.requireNonNull(token);
        this.state = State.IN_PROGRESS;
        this.createdAt = Objects.requireNonNull(now);
        this.updatedAt = now;
    }

    void reclaim(int nextAttempt, UUID token, Instant now) {
        if (state != State.RETRYABLE || nextAttempt <= attemptNumber) {
            throw new IllegalStateException("Only a later attempt may reclaim a retryable task");
        }
        attemptNumber = nextAttempt;
        claimToken = Objects.requireNonNull(token);
        state = State.IN_PROGRESS;
        updatedAt = Objects.requireNonNull(now);
    }

    void transition(State next, Instant now) {
        if (state != State.IN_PROGRESS || next == State.IN_PROGRESS) {
            throw new IllegalStateException("Invalid task checkpoint transition");
        }
        state = next;
        updatedAt = Objects.requireNonNull(now);
    }
}
