package com.flowforge.execution;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.UUID;

/** Immutable evidence trail for a manually verified remote task outcome. */
@Entity
@Table(name = "task_checkpoint_reconciliation_audit")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TaskCheckpointReconciliationAudit {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "checkpoint_id", nullable = false)
    private TaskExecutionCheckpoint checkpoint;
    @Column(name = "previous_attempt", nullable = false)
    private int previousAttempt;
    @Column(name = "previous_claim_token", nullable = false)
    private UUID previousClaimToken;
    @Enumerated(EnumType.STRING)
    @Column(name = "resolved_state", nullable = false, length = 20)
    private TaskExecutionCheckpoint.State resolvedState;
    @Column(name = "evidence_reference", nullable = false, length = 512)
    private String evidenceReference;
    @Column(name = "reconciled_at", nullable = false)
    private Instant reconciledAt;

    TaskCheckpointReconciliationAudit(TaskExecutionCheckpoint checkpoint,
            TaskExecutionCheckpoint.State resolvedState, String evidenceReference, Instant at) {
        this.checkpoint = checkpoint;
        this.previousAttempt = checkpoint.getAttemptNumber();
        this.previousClaimToken = checkpoint.getClaimToken();
        this.resolvedState = resolvedState;
        this.evidenceReference = evidenceReference;
        this.reconciledAt = at;
    }
}
