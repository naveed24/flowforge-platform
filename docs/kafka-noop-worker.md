# Kafka NOOP workflow worker (opt-in)

This first Kafka worker slice consumes durable execution dispatch events and
executes **NOOP-only** workflow DAGs. It is disabled by default, independently
of the outbox publisher. A NOOP task has no external side effects, so the
worker can mark the whole validated DAG as succeeded without invoking any
external systems. JAVA, HTTP and SHELL task executors are **not implemented**.

## Enable

Provision a Kafka broker and the `flowforge.execution.dispatch.v1` topic.
Then set `FLOWFORGE_DISPATCH_ENABLED=true`,
`FLOWFORGE_WORKER_ENABLED=true`, and `KAFKA_BOOTSTRAP_SERVERS` to the
broker address. The default consumer group is `flowforge-workers`; override
with `FLOWFORGE_WORKER_GROUP_ID` if required. Kafka auto-commit is disabled,
and offsets are acknowledged only after the database transaction commits.

The consumer validates the message key, the outbox row ID, execution ID and
attempt number before claiming the queued attempt. A conditional SQL update
allows only one worker to claim the current attempt. Claim and completion
share **one PostgreSQL transaction**: if the process dies or the transaction
fails before commit, the execution remains QUEUED and Kafka can redeliver it.
A duplicate, cancelled or stale retry-generation event is a no-op.

If a workflow includes any non-NOOP task, the attempt is recorded as failed
through the existing retry/backoff state machine. **Do not enable this worker
for production workflows containing JAVA, HTTP or SHELL tasks.** No external
task is executed or silently marked successful. Future workers will implement
those task types with Redis lease fencing and side-effect idempotency.

## Failure behavior and limitations

A permanently invalid dispatch (bad execution key, missing outbox intent,
mismatched attempt, or malformed JSON) is sent to the configured dead-letter
topic `flowforge.execution.dispatch.v1.DLT`. The default error handler treats
`IllegalArgumentException` and deserialization failures as non-retryable.
The DLT publisher **waits for broker acknowledgment**; only after a successful
send may the original offset be committed. A failed or timed-out DLT send
does not recover the record and it is retried, rather than silently skipped.

Database failures and other transient processing exceptions continue retrying
every five seconds without a finite retry limit; a broker outage therefore
cannot exhaust a fixed retry budget and discard legitimate work. A prolonged
database outage can still block a Kafka partition. Monitor consumer lag and
DLT volume. DLT records require operator investigation and explicit replay;
the DLT is **not** automatically consumed.

**Provision both Kafka topics before enabling the worker** and give the
consumer group read access to the dispatch topic and the producer write access
to the DLT. The DLT may have fewer partitions than the source topic because
the broker selects its partition. Override the DLT destination with
`FLOWFORGE_WORKER_DLT_TOPIC` when using a custom dispatch topic
(`FLOWFORGE_DISPATCH_TOPIC`). DLT records preserve their original key and
payload, including raw bytes for malformed JSON, plus Spring Kafka error
headers. Avoid routing DLT messages back to the dispatch topic.

Kafka broker integration is not part of the in-process test suite. Tests cover
transactional NOOP completion, duplicate delivery, unsupported-task retries,
outbox identity checks, acknowledgment ordering, malformed payload preservation
and dead-letter recovery behavior. Validate broker/topic provisioning, consumer
permissions and topic retention before enabling.
