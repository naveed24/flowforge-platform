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

The default heartbeat interval is 10000 ms. Configure
`flowforge.worker.lease.heartbeat-interval-ms` (environment
`FLOWFORGE_WORKER_LEASE_HEARTBEAT_INTERVAL_MS`) when changing the TTL.
The interval must be >= 100 ms and <= one third of the TTL. Invalid
configuration fails application startup. Start heartbeats immediately
after acquiring a lease, before executing the associated work.

Closing a heartbeat stops renewal but **does not release Redis ownership**.
Call the token-checked releaser separately in a finally block. The loss
callback runs on a heartbeat scheduler thread and should return promptly.
Spring shuts down the heartbeat scheduler when the application stops.

## Limitations

PostgreSQL conditional execution claims remain authoritative. Heartbeats
alone do not provide exactly-once execution, fencing of external side effects,
or recovery of stuck RUNNING jobs. Future worker integration must enforce
fencing and cooperative cancellation; an expired worker can otherwise
continue performing side effects.

Redis 7 integration tests verify contention, renewal, safe release and
configured TTL. Deterministic unit tests cover automatic renewal and
fail-closed lease-loss behavior. Local Redis defaults to localhost:6380.
