package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import java.util.concurrent.ScheduledExecutorService;

class WorkerLeaseHeartbeatServiceTest {
    @Test
    void invalidIntervalFailsFast() {
        var leases = mock(RedisWorkerLeaseService.class);
        var scheduler = mock(ScheduledExecutorService.class);
        assertThatThrownBy(() -> new WorkerLeaseHeartbeatService(leases, 1000, 1000, scheduler))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
