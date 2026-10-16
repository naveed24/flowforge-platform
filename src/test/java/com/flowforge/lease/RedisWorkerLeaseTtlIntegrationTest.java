package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "flowforge.worker.lease.ttl-ms=12000")
@ActiveProfiles("test")
@EnabledIfEnvironmentVariable(named = "FLOWFORGE_REDIS_TEST", matches = "true")
class RedisWorkerLeaseTtlIntegrationTest {
    @Autowired RedisWorkerLeaseService leases;
    @Autowired RedisLeaseReleaser releaser;
    @Autowired StringRedisTemplate redis;

    @Test
    void customTtlIsAppliedToAcquisitionAndAtomicRenewal() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        String key = "flowforge:execution:lease:" + id;
        var lease = leases.tryAcquire(id, "worker-ttl").orElseThrow();
        try {
            assertThat(redis.getExpire(key, TimeUnit.MILLISECONDS)).isBetween(1L, 12_000L);
            assertThat(redis.expire(key, Duration.ofSeconds(1))).isTrue();
            assertThat(leases.renew(lease)).isTrue();
            assertThat(redis.getExpire(key, TimeUnit.MILLISECONDS)).isBetween(5_000L, 12_000L);
        } finally {
            assertThat(releaser.release(lease)).isTrue();
        }
        assertThat(redis.hasKey(key)).isFalse();
    }
}
