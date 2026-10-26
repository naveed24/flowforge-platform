# Redis worker lease primitive

FlowForge provides a reusable Redis lease for distributed execution workers.
This is an **infrastructure primitive**, not a complete task runner.

`RedisWorkerLeaseService.tryAcquire(executionId, workerId)` uses Redis
`SET key token NX PX <ttl>` and returns a token-bearing `WorkerLease`.
`renew(lease)` atomically compares the token and extends the TTL using Lua.
`RedisLeaseReleaser.release(lease)` only removes a matching token.

## Configurable expiry and automatic heartbeat

Configure `flowforge.worker.lease.ttl-ms` (environment
`FLOWFORGE_WORKER_LEASE_TTL_MS`) from 1000 to 300000 ms; default 30000 ms.
Acquisition and renewal use the same TTL.

`WorkerLeaseHeartbeatService.start(lease, onLeaseLost)` schedules periodic
token-checked renewals and returns a closeable heartbeat handle. Renewal
failure, including Redis unavailability, permanently deactivates the handle
and invokes `onLeaseLost` at most once. The callback must promptly stop
processing; `isActive()` can be checked before further work.

By default the heartbeat interval is one third of the configured TTL
(10000 ms with the default 30000 ms TTL). Configure
`flowforge.worker.lease.heartbeat-interval-ms` (environment
`FLOWFORGE_WORKER_LEASE_HEARTBEAT_INTERVAL_MS`) when changing the TTL.
The interval must be >= 100 ms and <= one third of the TTL. Invalid
configuration fails application startup. Start heartbeats immediately
after acquiring a lease, before executing the associated work.

Closing a heartbeat stops renewal but **does not release Redis ownership**.
Call the token-checked releaser separately in a finally block. The loss
callback runs on a heartbeat scheduler thread and should return promptly.
Spring shuts down the heartbeat scheduler when the application stops.

## Kafka worker integration (opt-in)

The Kafka NOOP consumer can now use the lease primitive through
`WorkerDispatchLeaseCoordinator`. Set `FLOWFORGE_WORKER_ENABLED=true` and
`FLOWFORGE_WORKER_LEASE_ENABLED=true`, with Redis available at the configured
host/port. The coordinator acquires before entering the database transaction,
starts heartbeats, checks ownership after commit, and releases before Kafka
acknowledgment. Contention or lost ownership leaves the record unacknowledged
for retry; it does not dead-letter transient Redis failures. When leasing is
disabled, the existing PostgreSQL attempt-fenced worker behavior is unchanged.

The heartbeat callback records/logs lease loss, but it cannot interrupt a
synchronous database call. Final token verification and conditional release
prevent acknowledgment after known ownership loss. They do not roll back
already-committed work. The NOOP worker is safe to redeliver because it
performs no external side effects and database transitions are attempt-fenced.

## Limitations

PostgreSQL conditional execution claims remain authoritative. Heartbeats
alone do not provide exactly-once execution, fencing of external side effects,
or recovery of stuck RUNNING jobs. Future worker integration must enforce
fencing and cooperative cancellation; an expired worker can otherwise
continue performing side effects.

Redis 7 integration tests verify contention, renewal, safe release and
configured TTL. Deterministic unit tests cover automatic renewal and
fail-closed lease-loss behavior. Local Redis defaults to localhost:6380.
