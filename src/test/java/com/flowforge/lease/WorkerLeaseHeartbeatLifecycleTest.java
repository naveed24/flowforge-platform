package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkerLeaseHeartbeatLifecycleTest {
    @Test void renewsThenNotifiesOnceOnLostLease() {
        var leases = mock(RedisWorkerLeaseService.class);
        var scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        doReturn(future).when(scheduler).scheduleWithFixedDelay(
                any(Runnable.class), eq(200L), eq(200L), eq(TimeUnit.MILLISECONDS));
        var manager = new WorkerLeaseHeartbeatService(leases, 1000, 200, scheduler);
        var lease = new WorkerLease(42, "worker", "token");
        when(leases.renew(lease)).thenReturn(true, false);
        var losses = new AtomicInteger();
        var heartbeat = manager.start(lease, losses::incrementAndGet);
        var captor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).scheduleWithFixedDelay(captor.capture(), eq(200L), eq(200L),
                eq(TimeUnit.MILLISECONDS));
        captor.getValue().run();
        assertThat(heartbeat.isActive()).isTrue();
        captor.getValue().run();
        captor.getValue().run();
        assertThat(heartbeat.isActive()).isFalse();
        assertThat(losses.get()).isEqualTo(1);
        verify(leases, times(2)).renew(lease);
        verify(future).cancel(false);
    }
}
