package com.flowforge.lease;

/** Immutable capability used for compare-and-renew/release operations. */
public record WorkerLease(long executionId, String workerId, String token) {}
