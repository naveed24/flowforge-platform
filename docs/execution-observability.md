# Worker dispatch and stale recovery metrics

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

## Example PromQL

```promql
# Dispatch processing failures per second (transient failures may redeliver).
sum(rate(flowforge_worker_dispatch_total{result="failed"}[5m]))

# Number of failed stale-recovery polls in the last 15 minutes.
sum(increase(flowforge_execution_recovery_cycles_total{result="failed"}[15m]))

# Stale NOOP attempts recovered over the last hour.
sum(increase(flowforge_execution_recovery_executions_total[1h]))

# Failed retry scheduler polls over 15 minutes.
sum(increase(flowforge_execution_retry_cycles_total{result="failed"}[15m]))

# Due retries actually requeued over the last hour.
sum(increase(flowforge_execution_retry_requeued_total[1h]))

# Average dispatch listener duration in seconds over five minutes.
sum(rate(flowforge_worker_dispatch_duration_seconds_sum[5m]))
/
clamp_min(sum(rate(flowforge_worker_dispatch_duration_seconds_count[5m])), 1e-9)
```

Alert on sustained failed recovery polls, Kafka consumer lag, DLT growth, and
dispatch processing failures. Correlate these metrics with logs, PostgreSQL
execution state, and Redis availability. Counters reset on process restart;
use Prometheus `rate` or `increase` for alerting rather than raw differences.
