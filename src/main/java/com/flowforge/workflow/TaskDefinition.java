package com.flowforge.workflow;

import jakarta.persistence.*;
import lombok.*;

import java.util.LinkedHashSet;
import java.util.Set;

@Entity
@Table(
    name = "workflow_tasks",
    uniqueConstraints = @UniqueConstraint(name = "uk_task_workflow_key", columnNames = {"workflow_id", "task_key"})
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskDefinition {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "workflow_id", nullable = false)
    private WorkflowDefinition workflow;

    @Column(name = "task_key", nullable = false, length = 120)
    private String taskKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "task_type", nullable = false, length = 30)
    private TaskType taskType;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "workflow_task_dependencies",
        joinColumns = @JoinColumn(name = "task_id")
    )
    @Column(name = "depends_on_task_key", nullable = false, length = 120)
    @Builder.Default
    private Set<String> dependsOn = new LinkedHashSet<>();
}
