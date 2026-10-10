package com.flowforge.execution;

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

    @Query("select e from WorkflowExecution e where e.workflow.id = :workflowId and e.idempotencyKey = :key")
    Optional<WorkflowExecution> findIdempotentSubmission(
            @Param("workflowId") Long workflowId, @Param("key") String key);


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
