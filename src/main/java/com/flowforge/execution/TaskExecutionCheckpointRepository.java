package com.flowforge.execution;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TaskExecutionCheckpointRepository
        extends JpaRepository<TaskExecutionCheckpoint, Long> {

    Optional<TaskExecutionCheckpoint> findByExecution_IdAndTaskKey(
            Long executionId, String taskKey);
}
