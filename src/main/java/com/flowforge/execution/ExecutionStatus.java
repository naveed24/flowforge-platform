package com.flowforge.execution;

public enum ExecutionStatus {
    QUEUED,
    RUNNING,
    RETRY_WAIT,
    SUCCEEDED,
    FAILED,
    CANCELLED
}
