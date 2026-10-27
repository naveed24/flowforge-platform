package com.flowforge.execution;

import com.flowforge.workflow.TaskType;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class StaleRunningRecoveryServiceTest {
    private final WorkflowExecutionRepository repository = mock(WorkflowExecutionRepository.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-27T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void skipsRecoveryWhileRedisWorkerOwnsLease() {
        when(repository.findStaleNoopRunningIds(eq(ExecutionStatus.RUNNING),
                eq(TaskType.NOOP), any(Instant.class), any(Pageable.class)))
                .thenReturn(List.of(42L));
        when(redis.hasKey("flowforge:execution:lease:42")).thenReturn(true);

        assertThat(new StaleRunningRecoveryService(repository, redis, clock, 900_000, true)
                .recoverStaleNoopExecutions()).isZero();
        verify(redis).hasKey("flowforge:execution:lease:42");
        verify(repository, never()).findById(anyLong());
        verify(repository, never()).recoverStaleNoopRunningAttempt(
                anyLong(), anyInt(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void unknownRedisOwnershipFailsClosed() {
        when(repository.findStaleNoopRunningIds(any(), any(), any(), any()))
                .thenReturn(List.of(42L));
        when(redis.hasKey("flowforge:execution:lease:42")).thenReturn(null);

        assertThat(new StaleRunningRecoveryService(repository, redis, clock, 900_000, true)
                .recoverStaleNoopExecutions()).isZero();
        verify(repository, never()).findById(anyLong());
    }

    @Test
    void redisOutageNeverMarksAnAttemptFailed() {
        when(repository.findStaleNoopRunningIds(any(), any(), any(), any()))
                .thenReturn(List.of(42L));
        when(redis.hasKey("flowforge:execution:lease:42"))
                .thenThrow(new IllegalStateException("redis unavailable"));

        assertThatThrownBy(() -> new StaleRunningRecoveryService(
                repository, redis, clock, 900_000, true).recoverStaleNoopExecutions())
                .isInstanceOf(IllegalStateException.class);
        verify(repository, never()).findById(anyLong());
    }

    @Test
    void refusesUnsafeRecoveryTimeout() {
        assertThatThrownBy(() -> new StaleRunningRecoveryService(
                repository, redis, clock, 59_999, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StaleRunningRecoveryService(
                repository, redis, clock, 86_400_001, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
