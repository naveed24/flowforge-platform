package com.flowforge.lease;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkerLeaseHeartbeatLifecycleTest {
    private final RedisWorkerLeaseService leases = mock(RedisWorkerLeaseService.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final WorkerLease lease = new WorkerLease(42, "worker", "token");

    private Runnable start(AtomicInteger losses) {
        when(scheduler.scheduleWithFixedDelay(any(Runnable.class), eq(200L), eq(200L),
                eq(TimeUnit.MILLISECONDS))).thenReturn(future);
        new WorkerLeaseHeartbeatService(leases, 1000, 200, scheduler)
                .start(lease, losses::incrementAndGet);
        var captor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).scheduleWithFixedDelay(captor.capture(), eq(200L), eq(200L),
                eq(TimeUnit.MILLISECONDS));
        return captor.getValue();
    }

    @Test void renewsAndNotifiesOnceAfterLoss() {
        var losses = new AtomicInteger();
        when(leases.renew(lease)).thenReturn(true, false);
        var tick = start(losses);
        tick.run();
        assertThat(losses.get()).isZero();
        tick.run();
        tick.run();
        assertThat(losses.get()).isEqualTo(1);
        verify(leases, times(2)).renew(lease);
        verify(future).cancel(false);
    }

    @Test void redisFailureFailsClosed() {
        var losses = new AtomicInteger();
        when(leases.renew(lease)).thenThrow(new IllegalStateException("redis down"));
        var tick = start(losses);
        tick.run();
        tick.run();
        assertThat(losses.get()).isEqualTo(1);
        verify(leases).renew(lease);
        verify(future).cancel(false);
    }
}
