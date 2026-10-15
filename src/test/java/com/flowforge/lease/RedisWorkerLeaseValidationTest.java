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
        var releaser = new RedisLeaseReleaser(redis);
        for (var lease : new WorkerLease[] {null, new WorkerLease(0, "worker", "token"),
                new WorkerLease(1, " ", "token"), new WorkerLease(1, "worker", " ")}) {
            assertThatThrownBy(() -> leases.renew(lease)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> releaser.release(lease)).isInstanceOf(IllegalArgumentException.class);
        }
        verifyNoInteractions(redis);
    }
}
