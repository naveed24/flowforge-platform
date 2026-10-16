package com.flowforge.lease;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class RedisWorkerLeaseService {
    private static final long DEFAULT_TTL_MILLIS = 30_000;
    private static final long MIN_TTL_MILLIS = 1_000;
    private static final long MAX_TTL_MILLIS = 300_000;

    private final StringRedisTemplate redis;
    private final Duration leaseTtl;

    @Autowired
    public RedisWorkerLeaseService(
            StringRedisTemplate redis,
            @Value("${flowforge.worker.lease.ttl-ms:30000}") long ttlMillis) {
        if (ttlMillis < MIN_TTL_MILLIS || ttlMillis > MAX_TTL_MILLIS) {
            throw new IllegalArgumentException("Worker lease TTL must be between 1000 and 300000 ms");
        }
        this.redis = redis;
        this.leaseTtl = Duration.ofMillis(ttlMillis);
    }

    RedisWorkerLeaseService(StringRedisTemplate redis) {
        this(redis, DEFAULT_TTL_MILLIS);
    }

    public Optional<WorkerLease> tryAcquire(long id, String worker) {
        if (id <= 0 || worker == null || worker.isBlank()) {
            throw new IllegalArgumentException("Invalid lease identity");
        }
        String token = UUID.randomUUID().toString();
        Boolean ok = redis.opsForValue().setIfAbsent(
                key(id), token, leaseTtl);
        return Boolean.TRUE.equals(ok)
                ? Optional.of(new WorkerLease(id, worker, token))
                : Optional.empty();
    }

    public boolean renew(WorkerLease lease) {
        validateLease(lease);
        Long ok = redis.execute(RedisLeaseScripts.RENEW,
                List.of(key(lease.executionId())), lease.token(), Long.toString(leaseTtl.toMillis()));
        return Long.valueOf(1).equals(ok);
    }

    static void validateLease(WorkerLease lease) {
        if (lease == null || lease.executionId() <= 0 || lease.workerId() == null
                || lease.workerId().isBlank() || lease.token() == null || lease.token().isBlank()) {
            throw new IllegalArgumentException("Invalid worker lease");
        }
    }

    private String key(long id) {
        return "flowforge:execution:lease:" + id;
    }
}
