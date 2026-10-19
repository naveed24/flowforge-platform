# Transactional execution dispatch outbox

FlowForge now persists a **dispatch intent** whenever an execution becomes
`QUEUED`: at initial submission and when a due retry is requeued. The outbox
record is committed in the **same PostgreSQL transaction** as the execution
state change. A failure to persist the outbox rolls back the state transition.

## Schema and guarantees

Flyway V7 creates `execution_dispatch_outbox` with an immutable execution
reference, a one-based `attempt_number`, `created_at` and nullable
`published_at`. A foreign key prevents orphaned records; a unique constraint
on `(execution_id, attempt_number)` prevents duplicate dispatch intents for
the same attempt. An index on `(published_at, id)` supports bounded pending
scans. V7 backfills currently `QUEUED` executions from older deployments,
using `attempt_count + 1`; it does not redispatch historical running or
terminal executions.

- `ExecutionService.enqueue` writes the initial intent for attempt 1.
- Idempotent HTTP replays return the existing execution without another row.
- `DueRetryService.requeueDueRetries` writes the next intent only after its
  conditional `RETRY_WAIT -> QUEUED` update wins. Both writes are in one
  transaction, holding the execution row lock until commit.
- A cancelled or exhausted execution never creates a new retry intent.
- `ExecutionDispatchOutboxService` requires an existing transaction, rather
  than silently committing an outbox row independently.

## Next: Kafka publication

**This slice does not publish Kafka messages or start workers.** Rows remain
pending (`published_at IS NULL`) until a future dispatcher is implemented.
The dispatcher must claim pending records safely across instances, publish
`executionId` and `attemptNumber` using a stable Kafka message key, wait
for broker acknowledgment, then mark publication complete. A crash after Kafka
acknowledgment but before the database update can produce duplicates; the
worker must use `claimQueuedById` and the attempt-fenced completion API to
make duplicate deliveries harmless. Publication is **at-least-once**, not
exactly-once. A stale queued event must not resurrect cancelled or already
running work. Worker lease ownership and external side-effect fencing remain
separate requirements before running untrusted tasks.

Tests verify initial and idempotent submissions, retry requeue, cancellation,
retry exhaustion, and PostgreSQL migration schema.
