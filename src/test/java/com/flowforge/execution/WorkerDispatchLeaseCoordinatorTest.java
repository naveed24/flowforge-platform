package com.flowforge.execution;

import com.flowforge.lease.RedisLeaseReleaser;
import com.flowforge.lease.RedisWorkerLeaseService;
import com.flowforge.lease.WorkerLease;
import com.flowforge.lease.WorkerLeaseHeartbeatService;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkerDispatchLeaseCoordinatorTest {
    private final NoopWorkflowDispatchService worker = mock(NoopWorkflowDispatchService.class);
    private final RedisWorkerLeaseService leases = mock(RedisWorkerLeaseService.class);
    private final RedisLeaseReleaser releaser = mock(RedisLeaseReleaser.class);
    private final WorkerLeaseHeartbeatService heartbeats = mock(WorkerLeaseHeartbeatService.class);
    private final WorkerLeaseHeartbeatService.Heartbeat heartbeat =
            mock(WorkerLeaseHeartbeatService.Heartbeat.class);
    private final ExecutionDispatchMessage event = new ExecutionDispatchMessage(42, 1, 99);
    private final WorkerLease lease = new WorkerLease(42, "owner", "token");

    private WorkerDispatchLeaseCoordinator coordinator(boolean enabled) {
        return new WorkerDispatchLeaseCoordinator(worker, leases, releaser, heartbeats, enabled);
    }

    private void grantLease() {
        when(leases.tryAcquire(eq(42L), anyString())).thenReturn(Optional.of(lease));
        when(heartbeats.start(eq(lease), any(Runnable.class))).thenReturn(heartbeat);
        when(heartbeat.isActive()).thenReturn(true);
        when(leases.renew(lease)).thenReturn(true);
        when(releaser.release(lease)).thenReturn(true);
    }

    @Test
    void disabledModePreservesTransactionalWorkerBehaviorWithoutRedis() {
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        assertThat(coordinator(false).process(event))
                .isEqualTo(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        verifyNoInteractions(leases, releaser, heartbeats);
    }

    @Test
    void successfulWorkerIsVerifiedAndReleasedBeforeReturning() {
        grantLease();
        when(worker.process(event)).thenReturn(NoopWorkflowDispatchService.Outcome.SUCCEEDED);

        assertThat(coordinator(true).process(event))
                .isEqualTo(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        var ordered = inOrder(leases, heartbeats, worker, heartbeat, releaser);
        ordered.verify(leases).tryAcquire(eq(42L), anyString());
        ordered.verify(heartbeats).start(eq(lease), any(Runnable.class));
        ordered.verify(worker).process(event);
        ordered.verify(heartbeat).isActive();
        ordered.verify(leases).renew(lease);
        ordered.verify(heartbeat).close();
        ordered.verify(releaser).release(lease);
    }

    @Test
    void contentionNeverStartsOrAcknowledgesWork() {
        when(leases.tryAcquire(eq(42L), anyString())).thenReturn(Optional.empty());
        assertThatThrownBy(() -> coordinator(true).process(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("leased by another worker");
        verifyNoInteractions(worker, releaser, heartbeats);
    }

    @Test
    void lostOwnershipDuringHeartbeatFailsClosed() {
        grantLease();
        when(heartbeat.isActive()).thenReturn(false);
        assertThatThrownBy(() -> coordinator(true).process(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lost Redis lease");
        verify(leases, never()).renew(lease);
        verify(heartbeat).close();
        verify(releaser).release(lease);
    }

    @Test
    void failedFinalRenewalFailsClosed() {
        grantLease();
        when(leases.renew(lease)).thenReturn(false);
        assertThatThrownBy(() -> coordinator(true).process(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lost Redis lease");
        verify(heartbeat).close();
        verify(releaser).release(lease);
    }

    @Test
    void failedReleaseCannotSilentlyAcknowledge() {
        grantLease();
        when(releaser.release(lease)).thenReturn(false);
        assertThatThrownBy(() -> coordinator(true).process(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lost before release");
        verify(heartbeat).close();
    }

    @Test
    void databaseFailureStillClosesHeartbeatAndReleasesLease() {
        grantLease();
        when(worker.process(event)).thenThrow(new IllegalStateException("database down"));
        assertThatThrownBy(() -> coordinator(true).process(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database down");
        verify(heartbeat).close();
        verify(releaser).release(lease);
    }

    @Test
    void redisFailureDoesNotInvokeWorker() {
        when(leases.tryAcquire(eq(42L), anyString()))
                .thenThrow(new IllegalStateException("redis down"));
        assertThatThrownBy(() -> coordinator(true).process(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("redis down");
        verifyNoInteractions(worker, heartbeats, releaser);
    }

    @Test
    void invalidIdentityIsRejectedBeforeRedis() {
        assertThatThrownBy(() -> coordinator(true).process(
                new ExecutionDispatchMessage(0, 1, 99)))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(worker, leases, heartbeats, releaser);
    }
}
