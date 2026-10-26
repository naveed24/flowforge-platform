package com.flowforge.execution;

import com.flowforge.lease.RedisLeaseReleaser;
import com.flowforge.lease.RedisWorkerLeaseService;
import com.flowforge.lease.WorkerLeaseHeartbeatService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Redis ownership wraps (rather than joins) the transactional database worker.
 * The DB attempt claim remains authoritative; Redis limits concurrent workers
 * and provides a lease-loss signal for future long-running task executors.
 */
@Service
public class WorkerDispatchLeaseCoordinator {
    private static final Logger log = LoggerFactory.getLogger(WorkerDispatchLeaseCoordinator.class);

    private final NoopWorkflowDispatchService worker;
    private final RedisWorkerLeaseService leases;
    private final RedisLeaseReleaser releaser;
    private final WorkerLeaseHeartbeatService heartbeats;
    private final boolean leaseEnabled;
    private final String workerId = "kafka-" + UUID.randomUUID();

    public WorkerDispatchLeaseCoordinator(
            NoopWorkflowDispatchService worker,
            RedisWorkerLeaseService leases,
            RedisLeaseReleaser releaser,
            WorkerLeaseHeartbeatService heartbeats,
            @Value("${flowforge.worker.consumer.lease-enabled:false}") boolean leaseEnabled) {
        this.worker = worker;
        this.leases = leases;
        this.releaser = releaser;
        this.heartbeats = heartbeats;
        this.leaseEnabled = leaseEnabled;
    }

    public NoopWorkflowDispatchService.Outcome process(ExecutionDispatchMessage event) {
        if (event == null || event.executionId() <= 0
                || event.attemptNumber() <= 0 || event.outboxId() <= 0) {
            throw new IllegalArgumentException("Invalid dispatch event");
        }
        if (!leaseEnabled) return worker.process(event);

        var lease = leases.tryAcquire(event.executionId(), workerId)
                .orElseThrow(() -> new IllegalStateException(
                        "Execution " + event.executionId() + " is leased by another worker"));
        try {
            try (var heartbeat = heartbeats.start(lease, () ->
                    log.warn("Worker lost Redis lease for execution {}", event.executionId()))) {
                // The proxied worker commits its DB transaction before returning.
                var outcome = worker.process(event);
                // A synchronous token check closes the gap between the last
                // scheduled heartbeat and the Kafka offset acknowledgment.
                if (!heartbeat.isActive() || !leases.renew(lease)) {
                    throw new IllegalStateException(
                            "Worker lost Redis lease for execution " + event.executionId());
                }
                return outcome;
            }
        } finally {
            // Compare-and-delete cannot remove a successor's lease. A missing
            // lease is an error: Kafka must redeliver instead of acknowledging.
            if (!releaser.release(lease)) {
                throw new IllegalStateException(
                        "Worker lease was lost before release for execution " + event.executionId());
            }
        }
    }
}
