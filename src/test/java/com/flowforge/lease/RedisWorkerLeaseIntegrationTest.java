package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import java.util.concurrent.ThreadLocalRandom;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EnabledIfEnvironmentVariable(named = "FLOWFORGE_REDIS_TEST", matches = "true")
class RedisWorkerLeaseIntegrationTest {
    @Autowired RedisWorkerLeaseService leases;
    @Autowired RedisLeaseReleaser releaser;

    @Test
    void ownershipChecksAndReacquisition() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        var first = leases.tryAcquire(id, "worker-A").orElseThrow();
        assertThat(leases.tryAcquire(id, "worker-B")).isEmpty();
        var impostor = new WorkerLease(id, "worker-B", "incorrect-owner");
        assertThat(leases.renew(impostor)).isFalse();
        assertThat(releaser.release(impostor)).isFalse();
        assertThat(leases.renew(first)).isTrue();
        assertThat(releaser.release(first)).isTrue();
        assertThat(leases.renew(first)).isFalse();
        assertThat(leases.tryAcquire(id, "worker-B")).isPresent();
    }
}
