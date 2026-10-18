# Atomic worker claims and attempt-fenced completion

FlowForge exposes internal worker-facing primitives for **future Kafka
at-least-once delivery**. They do not run DAG tasks or publish Kafka messages.

## Claiming a queued execution

- `ExecutionClaimService.claimQueuedById(executionId)` targets one persisted
  execution ID. It atomically transitions only a `QUEUED` row to `RUNNING`.
  Duplicate deliveries, cancellation, terminal and retry-waiting rows return
  empty.
- `claimNextQueued()` remains a bounded oldest-first fallback, scanning up
  to 100 candidates per call.
- Both use the same conditional SQL update: increment `attempt_count` and
  optimistic `version`, set `started_at` and clear `next_attempt_at`.
  Only one competing claim can win.
- Never claim `RETRY_WAIT` directly. The retry scheduler requeues due rows;
  exhausted attempts are not claimable.

## Completing the claimed attempt

Retain the **one-based attempt number** returned by the successful claim.
`ExecutionAttemptCompletionService.succeed(id, attempt)` and
`fail(id, attempt)` return true only if a conditional database update
transitions that exact `RUNNING` attempt. Missing, cancelled, duplicate,
and stale attempts return false without changing state.

Failure persists `RETRY_WAIT` and exponential backoff when attempts remain,
or terminal `FAILED` and `finished_at` when exhausted. Success persists
`SUCCEEDED` and `finished_at`. Both increment optimistic `version`.
Do not refresh the attempt number from the database to complete older work.

## Delivery limitations

Attempt fencing distinguishes **retry generations**, not worker ownership.
If a Redis lease expires while the database still says `RUNNING`, its former
owner can still complete that same attempt. Redis lease heartbeats and
token-checked release exist as separate primitives but are not yet wired
into task execution. Before enabling distributed workers, integrate lease
ownership, cooperative cancellation, crash recovery and fencing for external
side effects. Kafka dispatch, outbox and task execution are not implemented;
a claimed execution can remain `RUNNING` after a worker crash.

H2 integration tests cover targeted claims, duplicate delivery, cancellation,
stale completions across retries and retry exhaustion.
