# Local Kafka broker and topic provisioning

The default `docker compose up -d postgres redis` remains unchanged. For
local end-to-end testing of the transactional outbox and NOOP Kafka worker,
start the **optional** Kafka profile:

```bash
docker compose --profile kafka up -d
docker compose --profile kafka ps
docker compose --profile kafka logs kafka-init
```

The profile starts a single-node Apache Kafka 3.9.1 KRaft broker and a
one-shot, idempotent provisioning job. The job creates both required topics:

| Topic | Partitions | Replication |
| --- | ---: | ---: |
| `flowforge.execution.dispatch.v1` | 3 | 1 |
| `flowforge.execution.dispatch.v1.DLT` | 3 | 1 |

Topic auto-creation is disabled so misspelled topics fail rather than silently
creating a new queue. Check that `kafka-init` exits with status 0 before
enabling the publisher or worker. A failed job must be investigated; it is
not safe to assume the DLT exists.

For a Spring Boot process running **on the host**, use:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export FLOWFORGE_DISPATCH_ENABLED=true
export FLOWFORGE_WORKER_ENABLED=true
# Optional: requires the existing Redis service.
export FLOWFORGE_WORKER_LEASE_ENABLED=true
mvn spring-boot:run
```

The host listener binds to `127.0.0.1:9092`. A process running inside
another Compose container must instead use `kafka:19092`; the two listeners
advertise different reachable addresses. Do not point container clients at
`localhost:9092`.

Only NOOP-only workflows can complete with the current worker. Workflows
containing HTTP, JAVA or SHELL tasks are **not executed** by this worker.
See [NOOP worker operations](kafka-noop-worker.md) and
[dispatch outbox design](execution-dispatch-outbox.md).

## Scope and production restrictions

This broker has one controller/broker, PLAINTEXT listeners, replication factor
one, and no authentication or durable Kafka volume. It is **for local
development only**, not production, high availability, or security testing.
Stopping/removing the broker can lose queued records; the database outbox
retains unacknowledged publication intents but cannot recreate records already
marked published. Production requires separately managed Kafka, access control,
durable replicated topics, retention policies, monitoring, and recovery plans.
