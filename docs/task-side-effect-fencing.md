# Durable task-side-effect admission and fencing

The task checkpoint primitive is a **foundation for future HTTP/JAVA/SHELL executors**, not an enabled external task runner. Existing Kafka workers remain NOOP-only. The new `execution_task_checkpoints` table (Flyway V9) has one row per **execution + task key**, shared across all retry attempts. A unique database constraint and a pessimistic lock on the parent execution serialize concurrent admissions.

## Admission contract

Call `TaskExecutionFenceService.tryClaim(executionId, taskKey, attemptNumber)` only after a workflow execution has been atomically claimed as RUNNING. It checks that the task belongs to the workflow, the execution is still RUNNING at the exact attempt, and the task is not already completed or unresolved. A successful claim returns a random claim token and a **stable SHA-256 idempotency key** for that execution/task. The key does not change when the workflow retries.

`succeed(claim)` and `markRetryable(claim)` accept only the exact token for the currently RUNNING attempt; stale workers, cancelled executions, duplicates, and prior retry generations cannot change the checkpoint. A successful checkpoint cannot be claimed again. These operations commit their own database transactions and do not authorize remote work after a lease is lost.

## Crash and retry safety

A new claim is stored as `IN_PROGRESS` **before** any external operation. If a worker crashes, times out, or loses its network connection after an unknown remote outcome, the checkpoint remains `IN_PROGRESS`. Later execution retries **must not** automatically replay it. The operator or a future reconciliation component must determine whether the remote operation took effect and resolve the ambiguity. This conservative behavior favors preventing duplicate side effects over automatic liveness.

Only if the executor can prove the remote operation **did not happen** may it call `markRetryable(claim)`, producing `RETRYABLE`. The next RUNNING execution attempt may then obtain a fresh token. The remote idempotency key stays the same. A successful external operation should call `succeed(claim)`, making the checkpoint terminal.

**Important limitation:** Database claim tokens fence *database checkpoint transitions*, not external systems. Redis lease tokens are also not remote fencing tokens. To approach effectively-once remote effects, future executors must send the stable key to a remote API that enforces idempotency, and implement explicit reconciliation for unknown outcomes. A database commit after a remote side effect can still fail. Do not assume exactly-once delivery or enable side-effecting executors until these integrations and operational controls exist.

The checkpoint FK cascades on execution deletion, but normal execution history should be retained. The `(state, updated_at)` index supports future investigation of unresolved `IN_PROGRESS` records. H2 integration tests cover admission, duplicate/forged completion, cancellation, ambiguous retries, and confirmed-safe retry generations; the PostgreSQL CI migration smoke test validates V9.
