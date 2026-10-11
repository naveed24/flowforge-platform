# Atomic execution claiming

`ExecutionClaimService.claimNextQueued()` is an internal worker-facing primitive
for moving an execution from `QUEUED` to `RUNNING`. It is not an HTTP endpoint
and does not run tasks or dispatch messages.

- Scan up to 100 oldest queued executions per call.
- Claim with a single conditional database update. Concurrent claimers cannot
  both transition the same row; a cancellation cannot be resurrected.
- Increment `attempt_count` and the optimistic `version`, set `started_at`,
  and clear `next_attempt_at` in the same transaction.
- Never claim `RETRY_WAIT` directly: the retry scheduler must requeue due
  attempts first. Exhausted attempts are not claimable.
- Return an empty result if no candidate wins the race. Workers may poll again.

**Delivery limitations:** This is a state-transition primitive, not a complete
distributed worker protocol. There is no Kafka dispatch, Redis lease, heartbeat,
automatic reclaim after worker crashes, or task execution yet. Do not enable
unbounded worker polling until delivery, lease recovery, and observability are
implemented. A claimed execution can remain `RUNNING` if its worker dies.

The H2 integration tests cover oldest-first claiming, exclusion of cancelled and
retry-waiting executions, and concurrent contenders. The existing CI runs Maven
tests and separately validates PostgreSQL Flyway migrations.
