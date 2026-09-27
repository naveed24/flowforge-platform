package com.flowforge.workflow;

import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowRepository extends JpaRepository<WorkflowDefinition, Long> {
    boolean existsByNameIgnoreCase(String name);
}
