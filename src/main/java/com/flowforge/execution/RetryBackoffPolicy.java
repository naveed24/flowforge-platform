package com.flowforge.execution;

import java.time.Duration;

/** Exponential retry delay with a bounded maximum and no integer overflow. */
public final class RetryBackoffPolicy {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long BASE_DELAY_SECONDS = 30;
    private static final long MAX_DELAY_SECONDS = 30 * 60;

    private RetryBackoffPolicy() {}

    /** @param failedAttempt one-based number of the attempt that just failed */
    public static Duration delayAfterFailure(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("Failed attempt must be positive");
        }
        long seconds = BASE_DELAY_SECONDS;
        for (int attempt = 1; attempt < failedAttempt && seconds < MAX_DELAY_SECONDS; attempt++) {
            seconds = Math.min(MAX_DELAY_SECONDS, seconds * 2);
        }
        return Duration.ofSeconds(seconds);
    }
}
