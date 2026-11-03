package com.flowforge.execution;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TaskCheckpointReconciliationAuditRepository
        extends JpaRepository<TaskCheckpointReconciliationAudit, Long> {
    long countByCheckpoint_Id(Long checkpointId);
}
