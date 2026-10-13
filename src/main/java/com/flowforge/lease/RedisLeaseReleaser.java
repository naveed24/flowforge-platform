package com.flowforge.lease;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class RedisLeaseReleaser {
    private final StringRedisTemplate redis;

    public RedisLeaseReleaser(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean release(WorkerLease lease) {
        Long ok = redis.execute(RedisLeaseScripts.RELEASE,
                List.of("flowforge:execution:lease:" + lease.executionId()), lease.token());
        return Long.valueOf(1).equals(ok);
    }
}
