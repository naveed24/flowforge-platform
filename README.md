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
- GitHub Actions (Maven tests, PostgreSQL/Flyway migration smoke test)

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


## Execution API (initial REST slice)

Workflow executions are persisted and can be submitted, inspected, and cancelled. An
execution must belong to an **ACTIVE** workflow. The scheduler/worker processing
the queued run is not yet exposed as a public API.

- `POST /api/v1/workflows/{workflowId}/executions` — submit a run; responds
  `201 Created` with an execution resource and `Location` header; optional
  `Idempotency-Key` header prevents duplicate submissions per workflow
- `GET /api/v1/executions/{executionId}` — read run status, version and timestamps
- `POST /api/v1/executions/{executionId}/cancel` — cancel a queued, running or retry-waiting run

A cancellation requires the *current execution version*, returned by GET or
submission, to prevent stale clients overwriting newer state:

```json
{"expectedVersion": 0}
```

A stale version returns `409 Conflict`, an invalid transition returns `400 Bad
Request`, and an unknown execution returns `404 Not Found`. Terminal executions
cannot be cancelled again. These endpoints are currently intended for local
development; production deployments require authentication and authorization
before exposing execution control.

## Durable execution retry state (current slice)

Executions persist `attemptCount`, `maxAttempts` (currently **3 total attempts**),
and `nextAttemptAt` in PostgreSQL, and expose them in the execution response.
Starting an attempt increments its count and records `startedAt`. When a
running attempt fails and retries remain, the execution enters `RETRY_WAIT`
instead of `FAILED`. The next attempt is eligible only when its persisted
`nextAttemptAt` is reached.

The retry policy uses **30 seconds, 60 seconds, then exponential doubling**,
capped at 30 minutes for future higher retry limits. Once the last attempt
fails, the execution becomes terminally `FAILED` with `finishedAt`. Successful
runs terminate in `SUCCEEDED`; cancellations of waiting runs clear the retry
timestamp and terminate in `CANCELLED`.

Retry eligibility is enforced by the execution service. A bounded background poller
now **automatically requeues due retries** from `RETRY_WAIT` to `QUEUED` at
five-second intervals (after a five-second startup delay). It processes up to
100 due executions per cycle, oldest due first. Requeueing uses a conditional
database update that increments the optimistic version, so overlapping pollers
cannot requeue the same retry twice or resurrect a cancelled execution.
The next worker start increments `attemptCount`, not the requeue operation.

Configure `flowforge.execution.retry.polling-enabled=false` to disable polling,
`flowforge.execution.retry.poll-interval-ms` to adjust the interval, and
`flowforge.execution.retry.poll-initial-delay-ms` to adjust the startup delay.
Tests disable the timer and exercise the polling service with a controlled clock.

### Idempotent execution submission

Include an optional `Idempotency-Key` header when submitting an execution:

```bash
curl -i -X POST http://localhost:8081/api/v1/workflows/1/executions \
  -H 'Idempotency-Key: checkout-123'
```

The first request creates an execution (HTTP 201). Replaying the same key for
the same workflow returns the **original** execution and its current status
(HTTP 200), even after the workflow is paused or the execution completes.
Different workflows may use the same key. Requests with no key still create
a fresh execution every time. Valid keys are 1-128 ASCII characters; they
must start with a letter or digit and may contain letters, digits, `.`,
`_`, `:`, or `-`. Invalid keys return HTTP 400.

PostgreSQL V5 adds a scoped unique index to reject concurrent duplicate
inserts. The API resolves a uniqueness race only after the losing
transaction rolls back, and never hides other database integrity failures.

**Important:** Requeueing is not worker dispatch. A separate worker/dispatcher
must claim and start queued executions. Distributed worker leases, task
execution, remain future milestones.

## CI verification

GitHub Actions runs the Spring/H2 unit and integration tests, plus a real
PostgreSQL 16 migration smoke test. The PostgreSQL check applies all Flyway
migrations to the isolated `flowforge_ci` database and verifies the workflow
schedule columns and execution idempotency index. Locally it is skipped
unless `FLOWFORGE_PG_TEST_URL` is provided. To prevent accidental data changes,
the test accepts only a localhost database named `flowforge_ci`.

## Seven-day roadmap

1. Foundation, workflow/task domain, DAG validation, persistence and containers
2. Rich workflow APIs, activation/versioning, scheduling metadata and tests
3. Execution engine, state machine, retries, backoff and idempotency
4. Kafka worker dispatch, Redis locking, leases and worker heartbeats
5. Cron scheduling, dead-letter handling, history and observability
6. React operations dashboard and live execution status
7. CI, security/config hardening, sample workflows, documentation and final polish

See [docs/architecture.md](docs/architecture.md).
