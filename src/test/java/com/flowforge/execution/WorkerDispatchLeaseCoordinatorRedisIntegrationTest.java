package com.flowforge.execution;

import com.flowforge.lease.RedisLeaseReleaser;
import com.flowforge.lease.RedisWorkerLeaseService;
import com.flowforge.lease.WorkerLeaseHeartbeatService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@EnabledIfEnvironmentVariable(named = "FLOWFORGE_REDIS_TEST", matches = "true")
class WorkerDispatchLeaseCoordinatorRedisIntegrationTest {
    @Autowired RedisWorkerLeaseService leases;
    @Autowired RedisLeaseReleaser releaser;
    @Autowired WorkerLeaseHeartbeatService heartbeats;

    @Test
    void redisOwnershipSurroundsWorkerAndIsReleasedAfterProcessing() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        var event = new ExecutionDispatchMessage(id, 1, 99);
        var worker = mock(NoopWorkflowDispatchService.class);
        when(worker.process(event)).thenAnswer(invocation -> {
            // A second worker must not acquire this execution during processing.
            assertThat(leases.tryAcquire(id, "contender")).isEmpty();
            return NoopWorkflowDispatchService.Outcome.SUCCEEDED;
        });
        var coordinator = new WorkerDispatchLeaseCoordinator(
                worker, leases, releaser, heartbeats, true);

        assertThat(coordinator.process(event))
                .isEqualTo(NoopWorkflowDispatchService.Outcome.SUCCEEDED);
        verify(worker).process(event);

        // The coordinator must release its token before the Kafka listener acks.
        var next = leases.tryAcquire(id, "successor").orElseThrow();
        assertThat(releaser.release(next)).isTrue();
    }

    @Test
    void contestedExecutionNeverInvokesWorker() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        var held = leases.tryAcquire(id, "first").orElseThrow();
        try {
            var worker = mock(NoopWorkflowDispatchService.class);
            var coordinator = new WorkerDispatchLeaseCoordinator(
                    worker, leases, releaser, heartbeats, true);
            assertThatThrownBy(() -> coordinator.process(
                    new ExecutionDispatchMessage(id, 1, 99)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("leased by another worker");
            verifyNoInteractions(worker);
        } finally {
            assertThat(releaser.release(held)).isTrue();
        }
    }
}
