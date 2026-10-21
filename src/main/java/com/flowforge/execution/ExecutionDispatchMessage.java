package com.flowforge.execution;

/** Stable v1 Kafka payload; executionId is also the partition key. */
public record ExecutionDispatchMessage(long executionId, int attemptNumber, long outboxId) {}
