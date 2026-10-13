package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import java.time.Duration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisWorkerLeaseContentionTest {
    @Test
    @SuppressWarnings("unchecked")
    void busyKeyDoesNotGrantLease() {
        var redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(eq("flowforge:execution:lease:42"),
                anyString(), eq(Duration.ofSeconds(30)))).thenReturn(false);
        assertThat(new RedisWorkerLeaseService(redis).tryAcquire(42, "worker"))
                .isEmpty();
    }
}
