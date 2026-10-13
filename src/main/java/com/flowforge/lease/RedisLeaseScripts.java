package com.flowforge.lease;

import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/** Server-side comparisons avoid GET-then-DEL and GET-then-EXPIRE races. */
final class RedisLeaseScripts {
    private RedisLeaseScripts() {}

    static final RedisScript<Long> RENEW = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end",
            Long.class);

    static final RedisScript<Long> RELEASE = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('DEL', KEYS[1]) else return 0 end",
            Long.class);
}
