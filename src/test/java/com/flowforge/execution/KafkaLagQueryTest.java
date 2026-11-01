package com.flowforge.execution;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaLagQueryTest {
    private static final String DISPATCH = "dispatch";
    private static final String DLT = "dispatch.DLT";
    private final TopicPartition first = new TopicPartition(DISPATCH, 0);
    private final TopicPartition second = new TopicPartition(DISPATCH, 1);
    private final TopicPartition deadLetter = new TopicPartition(DLT, 0);

    @Test
    void calculatesLagFromCommittedNextOffsetsAndRetainedDltRange() {
        var sample = KafkaLagQuery.calculate(DISPATCH, DLT,
                Map.of(first, 5L, second, 20L, deadLetter, 10L),
                Map.of(first, 30L, second, 50L, deadLetter, 17L),
                Map.of(first, 12L));

        // An uncommitted partition starts at earliest (worker auto-offset-reset).
        assertThat(sample.dispatchLag()).isEqualTo(18 + 30);
        assertThat(sample.dltRetained()).isEqualTo(7);
    }

    @Test
    void clampsOffsetsOutsideRetentionAndNeverReportsNegativeLag() {
        var sample = KafkaLagQuery.calculate(DISPATCH, DLT,
                Map.of(first, 10L, deadLetter, 7L),
                Map.of(first, 20L, deadLetter, 7L),
                Map.of(first, 100L));
        assertThat(sample.dispatchLag()).isZero();
        assertThat(sample.dltRetained()).isZero();

        var expiredCommit = KafkaLagQuery.calculate(DISPATCH, DLT,
                Map.of(first, 10L, deadLetter, 7L),
                Map.of(first, 20L, deadLetter, 9L),
                Map.of(first, 2L));
        assertThat(expiredCommit.dispatchLag()).isEqualTo(10);
        assertThat(expiredCommit.dltRetained()).isEqualTo(2);
    }

    @Test
    void rejectsIncompleteOrInvalidOffsetSamplesInsteadOfReportingZero() {
        assertThatThrownBy(() -> KafkaLagQuery.calculate(DISPATCH, DLT,
                Map.of(first, 0L, deadLetter, 0L),
                Map.of(first, 10L), Map.of()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> KafkaLagQuery.calculate(DISPATCH, DLT,
                Map.of(first, 20L, deadLetter, 0L),
                Map.of(first, 10L, deadLetter, 5L), Map.of()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> KafkaLagQuery.calculate(DISPATCH, DLT,
                Map.of(first, 0L), Map.of(first, 10L), Map.of()))
                .isInstanceOf(IllegalStateException.class);
    }
}
