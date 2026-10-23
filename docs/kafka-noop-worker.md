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

An invalid record or a database failure is not acknowledged. The Kafka error
handler retries every five seconds without silently skipping the offset. This
is fail-closed, but a poison record can block its partition until an operator
intervenes; dead-letter routing and bounded retries are future work.

Kafka broker integration is not part of the in-process test suite. Tests cover
transactional NOOP completion, duplicate delivery, unsupported-task retries,
outbox identity checks and acknowledgment ordering. Validate broker/topic
provisioning, consumer permissions and topic retention before enabling.
