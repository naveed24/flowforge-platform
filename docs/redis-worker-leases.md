# Redis worker lease primitive

FlowForge now has a reusable Redis lease for distributed execution workers.
This is an **infrastructure primitive**, not a complete task runner.

`RedisWorkerLeaseService.tryAcquire(executionId, workerId)` uses Redis
`SET key token NX PX <ttl>`: one owner can hold the lease at a time.
It returns an immutable `WorkerLease` containing a random UUID token.
The Redis key is `flowforge:execution:lease:<executionId>`.

`renew(lease)` atomically compares the token and extends the same configured
TTL using a Lua `GET` + `PEXPIRE` script. Callers should heartbeat
well before expiry (for example, every 10 seconds with the default TTL) and
**stop processing immediately** if renewal returns false or Redis is unavailable.

Set `FLOWFORGE_WORKER_LEASE_TTL_MS` (or Spring property
`flowforge.worker.lease.ttl-ms`) to choose a lease TTL in **milliseconds**.
The default is **30000 ms (30 seconds)**; allowed values are **1000–300000 ms**
(1 second to 5 minutes). Invalid values fail application startup rather than
silently creating leases that expire too quickly or live indefinitely. Acquire
and renew use exactly the same TTL. A heartbeat interval must remain well
below the configured TTL; no automatic worker heartbeat loop is implemented yet.

`RedisLeaseReleaser.release(lease)` atomically compares the token and
deletes the key using Lua. A stale worker cannot release a newer worker's
lease. Leases automatically expire after a crash, allowing recovery.

The existing PostgreSQL conditional execution claim remains the
authoritative transition from QUEUED to RUNNING. Redis lease ownership
alone does not guarantee exactly-once task execution, prevent side effects
from an expired worker, or recover RUNNING executions. The worker integration
must use database fencing and stop on lost heartbeats before executing
external side effects. No execution status is changed by these lease APIs.

The CI Redis integration tests verify contention, token ownership,
heartbeat renewal, safe release, reacquisition, and configured TTL renewal
against Redis 7.
Local application defaults use Redis on localhost:6380, as provided by
the existing docker-compose.yml.
