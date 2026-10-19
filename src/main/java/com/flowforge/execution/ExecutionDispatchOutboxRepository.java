package com.flowforge.execution;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ExecutionDispatchOutboxRepository extends JpaRepository<ExecutionDispatchOutbox, Long> {

    List<ExecutionDispatchOutbox> findByExecution_IdOrderByAttemptNumberAsc(Long executionId);

    /** Bounded, oldest-first pending events for a future Kafka publisher. */
    List<ExecutionDispatchOutbox> findByPublishedAtIsNullOrderByIdAsc(Pageable pageable);
}
