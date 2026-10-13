package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class RedisWorkerLeaseValidationTest {
    @Test
    void invalidIdentityIsRejectedWithoutContactingRedis() {
        var redis = mock(StringRedisTemplate.class);
        var leases = new RedisWorkerLeaseService(redis);
        assertThatThrownBy(() -> leases.tryAcquire(0, "worker"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> leases.tryAcquire(1, " "))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(redis);
    }
}
