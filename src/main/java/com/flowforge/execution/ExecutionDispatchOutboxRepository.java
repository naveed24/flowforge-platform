package com.flowforge.execution;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;

import java.util.List;

public interface ExecutionDispatchOutboxRepository extends JpaRepository<ExecutionDispatchOutbox, Long> {

    List<ExecutionDispatchOutbox> findByExecution_IdOrderByAttemptNumberAsc(Long executionId);

    /** Candidate scan is advisory; only the conditional UPDATE grants ownership. */
    @Query("""
            select o.id from ExecutionDispatchOutbox o
            where o.publishedAt is null
              and (o.claimedUntil is null or o.claimedUntil <= :now)
              and (o.nextPublishAt is null or o.nextPublishAt <= :now)
            order by o.id asc
            """)
    List<Long> findClaimableIds(@Param("now") Instant now, Pageable pageable);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update ExecutionDispatchOutbox o
            set o.claimToken = :token, o.claimedUntil = :until,
                o.publishAttempts = o.publishAttempts + 1, o.lastError = null
            where o.id = :id and o.publishedAt is null
              and (o.claimedUntil is null or o.claimedUntil <= :now)
              and (o.nextPublishAt is null or o.nextPublishAt <= :now)
            """)
    int claimForPublication(@Param("id") Long id, @Param("token") UUID token,
                            @Param("now") Instant now, @Param("until") Instant until);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update ExecutionDispatchOutbox o
            set o.publishedAt = :now, o.claimToken = null, o.claimedUntil = null,
                o.nextPublishAt = null, o.lastError = null
            where o.id = :id and o.claimToken = :token and o.publishedAt is null
            """)
    int acknowledge(@Param("id") Long id, @Param("token") UUID token,
                    @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update ExecutionDispatchOutbox o
            set o.claimToken = null, o.claimedUntil = null,
                o.nextPublishAt = :retryAt, o.lastError = :error
            where o.id = :id and o.claimToken = :token and o.publishedAt is null
            """)
    int releaseForRetry(@Param("id") Long id, @Param("token") UUID token,
                        @Param("retryAt") Instant retryAt, @Param("error") String error);

    /** Bounded, oldest-first pending events for a future Kafka publisher. */
    List<ExecutionDispatchOutbox> findByPublishedAtIsNullOrderByIdAsc(Pageable pageable);
}
