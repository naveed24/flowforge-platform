package com.flowforge.execution;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Short database transactions fence each outbox claim with a random token.
 * No transaction is held open while waiting for Kafka acknowledgments.
 */
@Service
public class ExecutionDispatchClaimService {
    private static final int SCAN_LIMIT = 100;
    private final ExecutionDispatchOutboxRepository repository;
    private final Clock clock;
    private final Duration claimLease;

    public ExecutionDispatchClaimService(
            ExecutionDispatchOutboxRepository repository, Clock clock,
            @Value("${flowforge.execution.dispatch.claim-lease-ms:60000}") long leaseMillis) {
        if (leaseMillis < 1000 || leaseMillis > 300000) {
            throw new IllegalArgumentException("Dispatch claim lease must be 1000-300000 ms");
        }
        this.repository = repository;
        this.clock = clock;
        this.claimLease = Duration.ofMillis(leaseMillis);
    }

    public record Claim(long outboxId, long executionId, int attemptNumber,
                        UUID token, int publishAttempts) {}

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<Claim> claimNext() {
        Instant now = clock.instant();
        for (Long id : repository.findClaimableIds(now, PageRequest.of(0, SCAN_LIMIT))) {
            UUID token = UUID.randomUUID();
            if (repository.claimForPublication(id, token, now, now.plus(claimLease)) == 1) {
                ExecutionDispatchOutbox row = repository.findById(id).orElseThrow();
                return Optional.of(new Claim(row.getId(), row.getExecution().getId(),
                        row.getAttemptNumber(), token, row.getPublishAttempts()));
            }
        }
        return Optional.empty();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markPublished(Claim claim) {
        Objects.requireNonNull(claim, "claim");
        return repository.acknowledge(claim.outboxId(), claim.token(), clock.instant()) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean releaseForRetry(Claim claim, String errorClass) {
        Objects.requireNonNull(claim, "claim");
        Objects.requireNonNull(errorClass, "errorClass");
        long shift = Math.min(Math.max(claim.publishAttempts() - 1, 0), 6);
        Duration backoff = Duration.ofSeconds(Math.min(300L, 5L << shift));
        String safeError = errorClass.substring(0, Math.min(120, errorClass.length()));
        Instant now = clock.instant();
        return repository.releaseForRetry(claim.outboxId(), claim.token(),
                now, now.plus(backoff), safeError) == 1;
    }
}
