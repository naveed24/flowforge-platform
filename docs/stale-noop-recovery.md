# Recovering orphaned NOOP worker attempts

FlowForge can optionally detect old `RUNNING` executions left behind when a
worker process crashes after committing its PostgreSQL claim but before
completing the attempt. Enable it with
`FLOWFORGE_STALE_RECOVERY_ENABLED=true`. The poller is **disabled by default**.

## Safety boundary

Recovery is intentionally restricted to workflows with at least one task and
**only NOOP tasks**. A timestamp alone cannot prove that a worker died. For
HTTP/JAVA/SHELL workflows, automatically restarting an attempt could repeat
irreversible external side effects. Those workflows remain `RUNNING` and
require operator intervention until task-level fencing and idempotency exist.

The default stale threshold is **15 minutes** measured from `started_at`.
Set `FLOWFORGE_STALE_RECOVERY_TIMEOUT_MS` between 60,000 and 86,400,000
milliseconds. Polling defaults to every 60 seconds with a 60-second initial
delay; override `FLOWFORGE_STALE_RECOVERY_POLL_INTERVAL_MS` and
`FLOWFORGE_STALE_RECOVERY_POLL_INITIAL_DELAY_MS`. Set the timeout comfortably
above expected worker execution and database pause times.

The poller scans at most 100 oldest eligible executions per cycle. Each
transition checks `RUNNING`, the exact attempt number, and the original
start-time cutoff in one conditional PostgreSQL update. A concurrent worker
completion, cancellation, or another recovery poller cannot overwrite a
changed row. Recovery increments the optimistic version.

If retries remain, the attempt enters `RETRY_WAIT` with the existing
exponential backoff. The existing due-retry scheduler later transitions it to
`QUEUED` and writes the **next attempt's outbox intent in the same database
transaction**. When attempts are exhausted, the run becomes terminal
`FAILED`. The recovery poller never creates a duplicate dispatch intent.

## Redis coordination and limitations

When `FLOWFORGE_WORKER_LEASE_ENABLED=true`, recovery checks the execution's
Redis lease key before transitioning it. An active lease is skipped; Redis
errors abort the recovery transaction, and an indeterminate lease response is
treated as owned. Keep the Redis lease service healthy when enabling recovery.

A Redis key check is **not** a distributed fencing transaction with PostgreSQL:
ownership may change between the check and SQL update. The conditional SQL
state/attempt fence is authoritative. This is safe for the current NOOP-only
worker because there are no external side effects, but it is not a guarantee
of exactly-once execution for future task types.

Monitor recovery warnings, retry backlog, execution ages and Redis health.
Do not enable this feature to reclaim non-NOOP work; those task types require
explicit side-effect idempotency and worker fencing first.
