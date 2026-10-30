package com.flowforge.execution;

import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Read-only, unbounded counts for the operational retry backlog.
 * Unlike the scheduler's bounded candidate scan, these values describe the
 * entire database backlog at the time of sampling.
 */
@Repository
@RequiredArgsConstructor
public class RetryBacklogQuery {
    private final EntityManager entityManager;

    public record Snapshot(long waiting, long due, Instant oldestDueAt) {}

    @Transactional(readOnly = true)
    public Snapshot read(Instant now) {
        long waiting = entityManager.createQuery("""
                select count(e) from WorkflowExecution e
                where e.status = :status
                """, Long.class)
                .setParameter("status", ExecutionStatus.RETRY_WAIT)
                .getSingleResult();
        long due = entityManager.createQuery("""
                select count(e) from WorkflowExecution e
                where e.status = :status and e.nextAttemptAt <= :now
                """, Long.class)
                .setParameter("status", ExecutionStatus.RETRY_WAIT)
                .setParameter("now", now)
                .getSingleResult();
        Instant oldestDue = entityManager.createQuery("""
                select min(e.nextAttemptAt) from WorkflowExecution e
                where e.status = :status and e.nextAttemptAt <= :now
                """, Instant.class)
                .setParameter("status", ExecutionStatus.RETRY_WAIT)
                .setParameter("now", now)
                .getSingleResult();
        return new Snapshot(waiting, due, oldestDue);
    }
}
