# FlowForge Platform

FlowForge is a production-style distributed workflow and background-job orchestration platform. It models how systems such as Temporal, Airflow, and internal job platforms define workflows, validate task dependencies, schedule executions, retry failures, and coordinate workers.

## Core goals

- Define workflows as directed acyclic graphs (DAGs)
- Persist workflow and task definitions
- Validate task dependencies before activation
- Execute workflows reliably with retries and idempotency
- Dispatch work asynchronously to workers
- Coordinate distributed workers with leases/locks
- Track execution history and operational metrics
- Provide an operations dashboard

## Tech stack

- Java 21
- Spring Boot 4
- Spring Web + Validation
- Spring Data JPA
- PostgreSQL + Flyway
- Redis
- Kafka (planned worker dispatch)
- Micrometer + Prometheus
- Docker / Docker Compose
- React (planned dashboard)
- GitHub Actions (planned CI)

## Day 1

The initial foundation contains:

- Spring Boot backend skeleton
- Workflow definition domain
- Task definition domain
- Workflow REST API
- DAG validation for missing dependencies and cycles
- PostgreSQL schema via Flyway
- Actuator + Prometheus
- Docker Compose for PostgreSQL and Redis
- H2 test profile
- Architecture documentation

## Run locally

```bash
docker compose up -d postgres redis
mvn spring-boot:run
```

Application: http://localhost:8081  
Health: http://localhost:8081/actuator/health  
Prometheus: http://localhost:8081/actuator/prometheus

## Example workflow

```json
{
  "name": "daily-order-report",
  "description": "Build and publish the daily order report",
  "tasks": [
    {
      "taskKey": "extract",
      "taskType": "HTTP",
      "dependsOn": []
    },
    {
      "taskKey": "transform",
      "taskType": "JAVA",
      "dependsOn": ["extract"]
    },
    {
      "taskKey": "publish",
      "taskType": "HTTP",
      "dependsOn": ["transform"]
    }
  ]
}
```

## Seven-day roadmap

1. Foundation, workflow/task domain, DAG validation, persistence and containers
2. Rich workflow APIs, activation/versioning, scheduling metadata and tests
3. Execution engine, state machine, retries, backoff and idempotency
4. Kafka worker dispatch, Redis locking, leases and worker heartbeats
5. Cron scheduling, dead-letter handling, history and observability
6. React operations dashboard and live execution status
7. CI, security/config hardening, sample workflows, documentation and final polish

See [docs/architecture.md](docs/architecture.md).
