# FlowForge architecture

## Day 1 architecture

```mermaid
flowchart LR
    Client[API Client / Future React UI] --> API[Spring Boot API]
    API --> Validator[DAG Validator]
    Validator --> PG[(PostgreSQL)]
    API --> Redis[(Redis)]
    API --> Metrics[Actuator / Prometheus]
```

## Target architecture

```mermaid
flowchart LR
    UI[React Operations Dashboard] --> API[FlowForge API]
    API --> Auth[Security / RBAC]
    API --> PG[(PostgreSQL)]
    API --> Scheduler[Scheduler]
    API --> Redis[(Redis)]
    Scheduler --> Kafka[(Kafka)]
    Kafka --> Worker1[Worker]
    Kafka --> Worker2[Worker]
    Kafka --> WorkerN[Worker]
    Worker1 --> Lease[Redis Lease / Lock]
    Worker2 --> Lease
    WorkerN --> Lease
    Worker1 --> History[Execution History]
    Worker2 --> History
    WorkerN --> History
    History --> PG
    Worker1 --> DLQ[Dead Letter Queue]
    Worker2 --> DLQ
    WorkerN --> DLQ
    API --> Metrics[Metrics / Tracing]
    Worker1 --> Metrics
    Worker2 --> Metrics
    WorkerN --> Metrics
```

## Reliability principles

1. Persist state before dispatching asynchronous work.
2. Treat worker delivery as at-least-once.
3. Make execution idempotent.
4. Use leases with expirations rather than permanent locks.
5. Model retry/backoff explicitly.
6. Preserve execution history for debugging.
7. Expose health and metrics by default.
