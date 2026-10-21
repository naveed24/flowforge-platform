package com.flowforge.execution;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface WorkflowExecutionRepository extends JpaRepository<WorkflowExecution, Long> {

    @Query("select e from WorkflowExecution e where e.workflow.id = :workflowId")
    Page<WorkflowExecution> findHistoryByWorkflowId(
            @Param("workflowId") Long workflowId, Pageable pageable);

    @Query("select e from WorkflowExecution e where e.workflow.id = :workflowId and e.status = :status")
    Page<WorkflowExecution> findHistoryByWorkflowIdAndStatus(
            @Param("workflowId") Long workflowId,
            @Param("status") ExecutionStatus status, Pageable pageable);

    @Query("select e from WorkflowExecution e where e.workflow.id = :workflowId and e.idempotencyKey = :key")
    Optional<WorkflowExecution> findIdempotentSubmission(
            @Param("workflowId") Long workflowId, @Param("key") String key);


    /**
     * Bounded oldest-first scan; competing workers may see the same candidates,
     * but only one can win the conditional claim below.
     */
    @Query("""
            select e.id from WorkflowExecution e
            where e.status = :queued and e.attemptCount < e.maxAttempts
            order by e.createdAt asc, e.id asc
            """)
    List<Long> findClaimableQueuedIds(
            @Param("queued") ExecutionStatus queued, Pageable pageable);

    /**
     * Atomic worker claim: a cancellation or another worker's claim wins if it
     * updates the row first. Increment the optimistic version for API clients.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update WorkflowExecution e
            set e.status = :running, e.attemptCount = e.attemptCount + 1,
                e.startedAt = :now, e.nextAttemptAt = null, e.version = e.version + 1
            where e.id = :id and e.status = :queued
              and e.attemptCount < e.maxAttempts
            """)
    int claimQueuedExecution(
            @Param("id") Long id,
            @Param("queued") ExecutionStatus queued,
            @Param("running") ExecutionStatus running,
            @Param("now") Instant now);

    /**
     * Reject stale Kafka messages from earlier retry generations.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update WorkflowExecution e
            set e.status = :running, e.attemptCount = e.attemptCount + 1,
                e.startedAt = :now, e.nextAttemptAt = null, e.version = e.version + 1
            where e.id = :id and e.status = :queued
              and e.attemptCount = :previousAttempt
              and e.attemptCount < e.maxAttempts
            """)
    int claimQueuedExecutionForAttempt(
            @Param("id") Long id,
            @Param("previousAttempt") int previousAttempt,
            @Param("queued") ExecutionStatus queued,
            @Param("running") ExecutionStatus running,
            @Param("now") Instant now);

    /**
     * Attempt-fenced completion. Does not validate Redis lease ownership.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update WorkflowExecution e
            set e.status = :target, e.nextAttemptAt = :nextAttemptAt,
                e.finishedAt = :finishedAt, e.version = e.version + 1
            where e.id = :id and e.status = :running
              and e.attemptCount = :expectedAttempt
            """)
    int completeRunningAttempt(
            @Param("id") Long id,
            @Param("expectedAttempt") int expectedAttempt,
            @Param("running") ExecutionStatus running,
            @Param("target") ExecutionStatus target,
            @Param("nextAttemptAt") Instant nextAttemptAt,
            @Param("finishedAt") Instant finishedAt);

    @Query("""
            select e.id from WorkflowExecution e
            where e.status = :status and e.nextAttemptAt <= :now
            order by e.nextAttemptAt asc, e.id asc
            """)
    List<Long> findDueRetryIds(
            @Param("status") ExecutionStatus status,
            @Param("now") Instant now,
            Pageable pageable);

    /**
     * Atomic conditional transition, safe when multiple schedulers race.
     * A concurrent cancellation cannot be changed back into QUEUED.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update WorkflowExecution e
            set e.status = :queued, e.nextAttemptAt = null, e.version = e.version + 1
            where e.id = :id and e.status = :waiting
              and e.nextAttemptAt is not null and e.nextAttemptAt <= :now
            """)
    int requeueDueRetry(
            @Param("id") Long id,
            @Param("waiting") ExecutionStatus waiting,
            @Param("queued") ExecutionStatus queued,
            @Param("now") Instant now);
}
