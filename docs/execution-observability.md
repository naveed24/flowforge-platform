# Execution and worker observability

FlowForge exposes bounded-cardinality Micrometer metrics through the existing
Spring Boot Actuator and Prometheus endpoint at
`GET /actuator/prometheus`. Worker metrics are emitted when
`FLOWFORGE_WORKER_ENABLED=true`; recovery metrics are emitted when
`FLOWFORGE_STALE_RECOVERY_ENABLED=true`. No execution IDs, worker IDs,
Kafka keys, exception messages, or workflow names are used as metric labels.

## Kafka consumer

| Micrometer meter | Prometheus series | Meaning |
| --- | --- | --- |
| `flowforge.worker.dispatch` (counter) | `flowforge_worker_dispatch_total{result="..."}` | Listener deliveries by outcome |
| `flowforge.worker.dispatch.duration` (timer) | `flowforge_worker_dispatch_duration_seconds_count` / `_sum` | Time spent validating, processing, and acknowledging a delivery |

The fixed `result` values are `succeeded`, `unsupported_retry`,
`duplicate_or_stale`, `invalid`, and `failed`.
`succeeded`, `unsupported_retry`, and `duplicate_or_stale` are recorded
only after the listener's manual acknowledgment returns successfully.
`invalid` means an `IllegalArgumentException` was raised while validating
or processing the dispatch. `failed` includes other processing, Redis lease,
and acknowledgment errors. These exceptions continue through the existing
Kafka error handler; metrics do not change retry or dead-letter policy.

**Scope:** these counters measure listener *delivery attempts*, not unique
executions or guaranteed broker commits. At-least-once redelivery may increment
them more than once for a single execution. Deserialization failures handled
before the listener (including malformed raw JSON sent directly to the DLT)
are not represented in these listener counters. Monitor DLT topic throughput
and consumer lag separately.

## Kafka dead-letter publishing

When `FLOWFORGE_WORKER_ENABLED=true`, the Kafka error handler reports
dead-letter publication outcomes, including deserialization failures that
never reach the dispatch listener. Both result series are registered at
startup, even before the first poison record.

| Micrometer meter | Prometheus series | Meaning |
| --- | --- | --- |
| `flowforge.worker.dead_letter.publish` (counter) | `flowforge_worker_dead_letter_publish_total{result="published|failed"}` | DLT publication attempts by broker-confirmed outcome |
| `flowforge.worker.dead_letter.publish.duration` (timer) | `flowforge_worker_dead_letter_publish_duration_seconds_count` / `_sum` | Time spent in the DLT recoverer, including waiting for the broker |

`published` increments **only after** the recoverer returns following a
successful broker-acknowledged send. `failed` increments if publication
throws, including broker failures and send timeouts. Failed publication
propagates back to the Kafka error handler; the source record is **not**
treated as recovered and is retried. Ordinary transient processing errors
do not enter the DLT recoverer and do not affect these counters.

These are **per-instance publication-attempt counters**, not unique poison
messages, retained DLT topic depth, consumer lag, or proof that a human
replayed a message. Retries may produce multiple failed attempts for one
source offset. Labels are fixed (`published`, `failed`); do not attach
exception text, topic, partition, key, or execution ID. A published record
still requires operator investigation and explicit replay.

## Kafka dispatch lag and retained dead-letter records (opt-in)

Set `FLOWFORGE_KAFKA_LAG_METRICS_ENABLED=true` to sample broker offsets
every 30 seconds; use `FLOWFORGE_KAFKA_LAG_METRICS_INTERVAL_MS` and
`FLOWFORGE_KAFKA_LAG_METRICS_INITIAL_DELAY_MS` to change the interval
and startup delay (milliseconds). The sampler is independent of the worker
and publisher switches and is **disabled by default**. Provision both topics
first. The Kafka AdminClient inherits Spring Kafka's broker/security
configuration and requires permission to describe both topics, read their
earliest/latest offsets and read committed offsets for the configured worker
consumer group.

| Micrometer meter | Prometheus series | Meaning |
| --- | --- | --- |
| `flowforge.kafka.dispatch.lag` (gauge) | `flowforge_kafka_dispatch_lag` | Sum of latest minus next committed offsets for dispatch partitions, clamped to retained range |
| `flowforge.kafka.dead_letter.retained` (gauge) | `flowforge_kafka_dead_letter_retained` | Number of records currently retained in the DLT, latest minus earliest across its partitions |
| `flowforge.kafka.offsets.last_success_timestamp` (gauge) | `flowforge_kafka_offsets_last_success_timestamp_seconds` | Unix timestamp of last complete successful broker sample |
| `flowforge.kafka.offsets.polls` (counter) | `flowforge_kafka_offsets_polls_total{result="succeeded|failed"}` | Successful and failed offset samples |

Uncommitted dispatch partitions start from the earliest **retained** offset,
matching the configured worker `auto-offset-reset: earliest`. Committed
offsets beyond the broker's current range are clamped to avoid negative lag.
Lag includes any retained records not yet acknowledged by the group; it is
not a count of unique workflow executions. The DLT gauge is **retained topic
depth**, not an unprocessed-message backlog: DLT records are not consumed
automatically, and Kafka retention can delete records without an operator
replaying them.

The poller fetches topic metadata, earliest/latest offsets, and group
commits with bounded timeouts. Scrapes read an atomic in-memory snapshot
and never call Kafka. Failed samples keep the last good values and timestamp,
and increment the failure counter; before the first successful sample the
gauges are zero, **not proof of an empty backlog**. A stale timestamp
must be alerted on. These are broker-wide gauges: do not sum them across
application replicas. Sampling may add broker load; increase the interval
for large clusters. The snapshot is approximate because offsets can advance
between admin calls. This telemetry is not an execution fencing mechanism.

```promql
# Current consumer-group dispatch lag (not summed across replicas).
max(flowforge_kafka_dispatch_lag)

# Retained DLT records requiring investigation.
max(flowforge_kafka_dead_letter_retained)

# Stale/uninitialized offset sampler (e.g. missing topic or Kafka ACL).
time() - max(flowforge_kafka_offsets_last_success_timestamp_seconds) > 120

# Kafka admin sampling failures in the last 15 minutes.
sum(increase(flowforge_kafka_offsets_polls_total{result="failed"}[15m]))
```

## Stale NOOP recovery

| Micrometer meter | Prometheus series | Meaning |
| --- | --- | --- |
| `flowforge.execution.recovery.cycles` (counter) | `flowforge_execution_recovery_cycles_total{result="..."}` | Successful or failed scheduler polls |
| `flowforge.execution.recovery.executions` (counter) | `flowforge_execution_recovery_executions_total` | Successfully recovered stale attempts |
| `flowforge.execution.recovery.cycle.duration` (timer) | `flowforge_execution_recovery_cycle_duration_seconds_count` / `_sum` | Scheduler poll duration |

Recovery cycle `result` has only `succeeded` and `failed`. Recovered
attempts are counted **after the recovery service transaction returns**, not
when candidates are merely discovered. A failed cycle never increments the
recovered-attempt counter. An empty successful poll increments the successful
cycle counter but not recovered attempts. Recovery remains NOOP-only and
disabled by default.

## Due retry scheduler

| Micrometer meter | Prometheus series | Meaning |
| --- | --- | --- |
| `flowforge.execution.retry.cycles` (counter) | `flowforge_execution_retry_cycles_total{result="..."}` | Successful or failed retry release polls |
| `flowforge.execution.retry.requeued` (counter) | `flowforge_execution_retry_requeued_total` | Retry attempts committed back to QUEUED with their dispatch outbox records |
| `flowforge.execution.retry.cycle.duration` (timer) | `flowforge_execution_retry_cycle_duration_seconds_count` / `_sum` | Duration of retry release polls |

The only retry cycle `result` labels are `succeeded` and `failed`.
An empty scan counts as a successful poll but does not increment the
requeued counter. Failed transactions (including failures to persist the
outbox intent) count as failed polls, **not** successful requeues. These
metrics are emitted when the retry poller is enabled (default); tests
disable the scheduler. A requeue is not a successful execution: dispatch
and worker processing happen later. The requeued counter can increase more
than once for an execution that fails across multiple attempts.

These counters measure *this application's* scheduler activity, not
global retry backlog or Kafka consumer lag. If several application replicas
run the poller, aggregate counters across replicas with Prometheus.
The transactional conditional update prevents concurrent pollers from
claiming the same due retry, but there is no guarantee of exactly-once
task execution.

## Database retry backlog (opt-in)

Set `FLOWFORGE_RETRY_BACKLOG_METRICS_ENABLED=true` to enable a read-only
PostgreSQL snapshot every 30 seconds. Change the interval with
`FLOWFORGE_RETRY_BACKLOG_METRICS_INTERVAL_MS` and initial delay with
`FLOWFORGE_RETRY_BACKLOG_METRICS_INITIAL_DELAY_MS` (both milliseconds).
This sampler is independent of the due-retry scheduler: it can detect
backlogs even when automatic requeueing is disabled.

| Micrometer meter | Prometheus series | Meaning |
| --- | --- | --- |
| `flowforge.execution.retry.backlog` (gauge) | `flowforge_execution_retry_backlog{state="waiting|due"}` | Total RETRY_WAIT rows and those with nextAttemptAt <= snapshot time |
| `flowforge.execution.retry.backlog.oldest_due_age` (gauge) | `flowforge_execution_retry_backlog_oldest_due_age_seconds` | Age in seconds of the oldest due retry at snapshot time, or zero if none |
| `flowforge.execution.retry.backlog.last_success_timestamp` (gauge) | `flowforge_execution_retry_backlog_last_success_timestamp_seconds` | Unix timestamp of the last successful sample |
| `flowforge.execution.retry.backlog.polls` (counter) | `flowforge_execution_retry_backlog_polls_total{result="succeeded|failed"}` | Snapshot successes and failures |

The sampler queries PostgreSQL on a fixed-delay schedule; **Prometheus scrapes
never query the database**. All gauges are published from a single immutable
snapshot. A failed query preserves the last good values and timestamp, and
increments the failed poll counter instead of reporting a false zero.
Before the first successful sample the gauges start at zero, including the
last-success timestamp; do not treat backlog zero as healthy until the
last-success timestamp is positive and recent.

These are database-wide counts, not per-worker counts. **Do not sum the
backlog gauges across replicas**; use `max` or select one healthy instance
instead. The queries are read-only but may scan many rows, so monitor database
load and increase the sampling interval on large deployments. Concurrent
updates can make the separate count/min queries an approximate snapshot;
this is operational telemetry, not an accounting or fencing mechanism.
Oldest-due age is measured when sampled, not continuously between polls.

## Example PromQL

```promql
# Dispatch processing failures per second (transient failures may redeliver).
sum(rate(flowforge_worker_dispatch_total{result="failed"}[5m]))

# Poison records successfully sent to the DLT over 15 minutes.
sum(increase(flowforge_worker_dead_letter_publish_total{result="published"}[15m]))

# Failed DLT sends over 15 minutes: investigate broker/topic/ACL health.
sum(increase(flowforge_worker_dead_letter_publish_total{result="failed"}[15m]))

# Number of failed stale-recovery polls in the last 15 minutes.
sum(increase(flowforge_execution_recovery_cycles_total{result="failed"}[15m]))

# Stale NOOP attempts recovered over the last hour.
sum(increase(flowforge_execution_recovery_executions_total[1h]))

# Failed retry scheduler polls over 15 minutes.
sum(increase(flowforge_execution_retry_cycles_total{result="failed"}[15m]))

# Due retries actually requeued over the last hour.
sum(increase(flowforge_execution_retry_requeued_total[1h]))

# Current due retry backlog across replicas (do not sum identical DB snapshots).
max(flowforge_execution_retry_backlog{state="due"})

# Alert when no successful backlog sample for 2 minutes, after initialization.
time() - max(flowforge_execution_retry_backlog_last_success_timestamp_seconds) > 120

# Age of the oldest due retry (sampled, not live).
max(flowforge_execution_retry_backlog_oldest_due_age_seconds)

# Average dispatch listener duration in seconds over five minutes.
sum(rate(flowforge_worker_dispatch_duration_seconds_sum[5m]))
/
clamp_min(sum(rate(flowforge_worker_dispatch_duration_seconds_count[5m])), 1e-9)
```

Alert on sustained failed recovery polls, Kafka consumer lag, DLT growth, and
dispatch processing failures. Correlate these metrics with logs, PostgreSQL
execution state, and Redis availability. Counters reset on process restart;
use Prometheus `rate` or `increase` for alerting rather than raw differences.
