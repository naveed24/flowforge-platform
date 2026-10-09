package com.flowforge.execution;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryBackoffPolicyTest {

    @Test
    void doublesDelayAndCapsAtThirtyMinutes() {
        assertThat(RetryBackoffPolicy.delayAfterFailure(1).toSeconds()).isEqualTo(30);
        assertThat(RetryBackoffPolicy.delayAfterFailure(2).toSeconds()).isEqualTo(60);
        assertThat(RetryBackoffPolicy.delayAfterFailure(3).toSeconds()).isEqualTo(120);
        assertThat(RetryBackoffPolicy.delayAfterFailure(8).toMinutes()).isEqualTo(30);
        assertThat(RetryBackoffPolicy.delayAfterFailure(Integer.MAX_VALUE).toMinutes()).isEqualTo(30);
    }

    @Test
    void rejectsInvalidAttemptNumbers() {
        assertThatThrownBy(() -> RetryBackoffPolicy.delayAfterFailure(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
