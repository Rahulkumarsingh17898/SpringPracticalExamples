package com.RKSCTO.SpringLearningSeries.notification.service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisRateLimiter {
    private final StringRedisTemplate redisTemplate;
    private final int limitPerMinute = 100;

    public RedisRateLimiter(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean tryConsume(String tenantId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        String key = "rate:" + tenantId + ":" + now;
        Long count = redisTemplate.opsForValue().increment(key);
        redisTemplate.expire(key, Duration.ofMinutes(1));
        return count != null && count <= limitPerMinute;
    }
}
