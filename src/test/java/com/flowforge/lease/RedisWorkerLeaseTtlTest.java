package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisWorkerLeaseTtlTest {

    @Test
    @SuppressWarnings("unchecked")
    void acquisitionAndRenewalUseSameConfiguredTtl() {
        var redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(eq("flowforge:execution:lease:42"),
                anyString(), eq(Duration.ofMillis(45_000)))).thenReturn(true);

        var service = new RedisWorkerLeaseService(redis, 45_000);
        var lease = service.tryAcquire(42, "worker-A").orElseThrow();

        assertThat(lease.workerId()).isEqualTo("worker-A");
        assertThat(lease.token()).isNotBlank();
        service.renew(lease);

        verify(values).setIfAbsent(eq("flowforge:execution:lease:42"),
                eq(lease.token()), eq(Duration.ofMillis(45_000)));
        verify(redis).execute(eq(RedisLeaseScripts.RENEW),
                eq(List.of("flowforge:execution:lease:42")),
                eq(lease.token()), eq("45000"));
    }

    @Test
    void invalidTtlFailsFastWithoutRedisAccess() {
        var redis = mock(StringRedisTemplate.class);
        for (long ttl : new long[] {-1, 0, 999, 300_001, Long.MAX_VALUE}) {
            assertThatThrownBy(() -> new RedisWorkerLeaseService(redis, ttl))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("between 1000 and 300000 ms");
        }
        verifyNoInteractions(redis);
    }

    @Test
    @SuppressWarnings("unchecked")
    void defaultTtlRemainsThirtySeconds() {
        var redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        new RedisWorkerLeaseService(redis).tryAcquire(11, "worker");
        verify(values).setIfAbsent(eq("flowforge:execution:lease:11"),
                anyString(), eq(Duration.ofSeconds(30)));
    }
}
