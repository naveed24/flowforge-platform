package com.flowforge.lease;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Automatically renews Redis worker leases and reports loss of ownership. */
@Service
public class WorkerLeaseHeartbeatService implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(WorkerLeaseHeartbeatService.class);
    private final RedisWorkerLeaseService leases;
    private final ScheduledExecutorService scheduler;
    private final long intervalMillis;

    @Autowired
    public WorkerLeaseHeartbeatService(
            RedisWorkerLeaseService leases,
            @Value("${flowforge.worker.lease.ttl-ms:30000}") long ttlMillis,
            @Value("${flowforge.worker.lease.heartbeat-interval-ms:10000}") long intervalMillis) {
        this(leases, ttlMillis, intervalMillis, Executors.newScheduledThreadPool(2, r -> {
            Thread thread = new Thread(r, "flowforge-worker-lease-heartbeat");
            thread.setDaemon(true);
            return thread;
        }));
    }

    WorkerLeaseHeartbeatService(RedisWorkerLeaseService leases, long ttlMillis,
                                long intervalMillis, ScheduledExecutorService scheduler) {
        if (ttlMillis < 1000 || ttlMillis > 300000 ||
                intervalMillis < 100 || intervalMillis > ttlMillis / 3) {
            throw new IllegalArgumentException("Heartbeat interval must be between 100 ms and one third of lease TTL");
        }
        this.leases = Objects.requireNonNull(leases);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.intervalMillis = intervalMillis;
    }

    /** onLeaseLost must promptly stop processing; close only stops renewal, not release. */
    public Heartbeat start(WorkerLease lease, Runnable onLeaseLost) {
        RedisWorkerLeaseService.validateLease(lease);
        Objects.requireNonNull(onLeaseLost, "onLeaseLost");
        Heartbeat heartbeat = new Heartbeat(leases, lease, onLeaseLost);
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
                heartbeat::tick, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        heartbeat.attach(future);
        return heartbeat;
    }

    @Override public void destroy() {
        scheduler.shutdownNow();
    }

    public static final class Heartbeat implements AutoCloseable {
        private final RedisWorkerLeaseService leases;
        private final WorkerLease lease;
        private final Runnable onLeaseLost;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>();

        private Heartbeat(RedisWorkerLeaseService leases, WorkerLease lease, Runnable onLeaseLost) {
            this.leases = leases;
            this.lease = lease;
            this.onLeaseLost = onLeaseLost;
        }

        public boolean isActive() { return active.get(); }

        private void attach(ScheduledFuture<?> task) {
            future.set(task);
            if (!active.get()) task.cancel(false);
        }

        private void tick() {
            if (!active.get()) return;
            boolean renewed;
            try {
                renewed = leases.renew(lease);
            } catch (RuntimeException e) {
                log.warn("Worker lease renewal failed for execution {}", lease.executionId(), e);
                renewed = false;
            }
            if (!renewed) stop(true);
        }

        private void stop(boolean lost) {
            if (!active.compareAndSet(true, false)) return;
            ScheduledFuture<?> task = future.get();
            if (task != null) task.cancel(false);
            if (lost) {
                try {
                    onLeaseLost.run();
                } catch (RuntimeException e) {
                    log.error("Lease-loss callback failed for execution {}", lease.executionId(), e);
                }
            }
        }

        @Override public void close() { stop(false); }
    }
}
