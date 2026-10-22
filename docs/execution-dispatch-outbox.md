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

## Kafka publication (opt-in)

Flyway V8 adds an expiring, token-fenced outbox claim, a publish-attempt counter,
retry timestamp and a bounded error class. Each polling cycle:

1. Scans at most 100 eligible rows, then atomically claims a row with a random
   UUID token and a 60-second lease. Concurrent instances cannot claim the same
   row until that lease expires. Claims commit **before** Kafka I/O.
2. Sends a JSON `ExecutionDispatchMessage(executionId, attemptNumber, outboxId)`
   to `flowforge.execution.dispatch.v1` with the execution ID as the Kafka
   partition key. The producer uses `acks=all` and idempotence.
3. Waits for the broker acknowledgment (default timeout 10 seconds) before
   conditionally setting `published_at` and clearing the claim. Only the
   current claim token **with an unexpired lease** may acknowledge or release
   a row. At the exact lease expiry instant, the claim is no longer valid.
4. On a send failure, releases the claim with exponential publication backoff
   (5, 10, 20 ... up to 300 seconds). A crash or database outage leaves a
   pending row for reclaim after the lease expires. Failure callbacks from an
   expired claim cannot reschedule retries or overwrite that row. Failures do
   not discard the dispatch intent.

The publisher is **disabled by default**. Provision a Kafka broker and the
`flowforge.execution.dispatch.v1` topic first, then set
`FLOWFORGE_DISPATCH_ENABLED=true` and
`KAFKA_BOOTSTRAP_SERVERS=host:9092`. Optionally configure
`FLOWFORGE_DISPATCH_TOPIC`. Application properties under
`flowforge.execution.dispatch` control the bounded batch size, lease duration,
ack timeout and polling interval. The acknowledgment timeout must be less than
the claim lease; otherwise startup rejects the configuration.

**At-least-once guarantee, not exactly-once:** a process can crash after Kafka
acknowledges but before the database marks publication complete. The message
may then be republished. Consumers **must** call
`ExecutionClaimService.claimQueuedById(executionId, attemptNumber)`, not the
legacy ID-only claim method. The attempt-aware conditional database update
rejects duplicates, cancellations and stale events from prior retries even
when a later attempt is now queued. Completion must likewise use the returned
attempt number. These safeguards do not fence external side effects.

**Scope boundary:** this milestone publishes durable dispatch events, but does
not consume Kafka messages, execute DAG tasks, or integrate Redis lease
ownership into task execution. Do not treat a published event as completed
work. Topic provisioning, worker consumers, crash recovery for RUNNING jobs,
dead-letter handling, metrics and external side-effect fencing are future
milestones. If Kafka is unavailable, pending outbox rows accumulate rather
than being lost.

Tests cover broker-acknowledged publication, publish failure/backoff, expired
lease reclamation, expired-token acknowledgment/retry rejection, late Kafka
acknowledgment fencing, stale token fencing, retry-generation fencing and the
PostgreSQL migration.
