package com.flowforge.lease;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class RedisWorkerLeaseService {
    private final StringRedisTemplate redis;

    public RedisWorkerLeaseService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Optional<WorkerLease> tryAcquire(long id, String worker) {
        if (id <= 0 || worker == null || worker.isBlank()) {
            throw new IllegalArgumentException("Invalid lease identity");
        }
        String token = UUID.randomUUID().toString();
        Boolean ok = redis.opsForValue().setIfAbsent(
                key(id), token, Duration.ofSeconds(30));
        return Boolean.TRUE.equals(ok)
                ? Optional.of(new WorkerLease(id, worker, token))
                : Optional.empty();
    }

    public boolean renew(WorkerLease lease) {
        Long ok = redis.execute(RedisLeaseScripts.RENEW,
                List.of(key(lease.executionId())), lease.token(), "30000");
        return Long.valueOf(1).equals(ok);
    }

    private String key(long id) {
        return "flowforge:execution:lease:" + id;
    }
}
