package com.flowforge.workflow;

import com.flowforge.common.BadRequestException;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class DagValidator {

    public void validate(List<WorkflowDtos.TaskRequest> tasks) {
        Map<String, Set<String>> graph = new LinkedHashMap<>();

        for (WorkflowDtos.TaskRequest task : tasks) {
            String key = task.taskKey().trim();

            if (graph.containsKey(key)) {
                throw new BadRequestException("Duplicate task key: " + key);
            }

            Set<String> dependencies = task.dependsOn() == null
                    ? Set.of()
                    : new LinkedHashSet<>(task.dependsOn());

            if (dependencies.contains(key)) {
                throw new BadRequestException("Task cannot depend on itself: " + key);
            }

            graph.put(key, dependencies);
        }

        for (Map.Entry<String, Set<String>> entry : graph.entrySet()) {
            for (String dependency : entry.getValue()) {
                if (!graph.containsKey(dependency)) {
                    throw new BadRequestException(
                            "Task '" + entry.getKey() + "' depends on unknown task '" + dependency + "'");
                }
            }
        }

        detectCycle(graph);
    }

    private void detectCycle(Map<String, Set<String>> graph) {
        Set<String> visiting = new HashSet<>();
        Set<String> visited = new HashSet<>();

        for (String node : graph.keySet()) {
            visit(node, graph, visiting, visited);
        }
    }

    private void visit(
            String node,
            Map<String, Set<String>> graph,
            Set<String> visiting,
            Set<String> visited) {

        if (visited.contains(node)) return;

        if (!visiting.add(node)) {
            throw new BadRequestException("Workflow contains a dependency cycle involving: " + node);
        }

        for (String dependency : graph.getOrDefault(node, Set.of())) {
            visit(dependency, graph, visiting, visited);
        }

        visiting.remove(node);
        visited.add(node);
    }
}
