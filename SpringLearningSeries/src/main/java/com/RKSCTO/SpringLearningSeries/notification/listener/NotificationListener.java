package com.RKSCTO.SpringLearningSeries.notification.listener;

import com.RKSCTO.SpringLearningSeries.notification.model.Notification;
import com.RKSCTO.SpringLearningSeries.notification.service.NotificationDispatcher;
import com.RKSCTO.SpringLearningSeries.notification.service.RedisRateLimiter;
import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {
    private final RedisRateLimiter rateLimiter;
    private final StringRedisTemplate redisTemplate;
    private final NotificationDispatcher dispatcher;

    public NotificationListener(RedisRateLimiter rateLimiter,
                                StringRedisTemplate redisTemplate,
                                NotificationDispatcher dispatcher) {
        this.rateLimiter = rateLimiter;
        this.redisTemplate = redisTemplate;
        this.dispatcher = dispatcher;
    }

    @KafkaListener(topics = "notifications", containerFactory = "kafkaListenerContainerFactory")
    public void listen(Notification notification, Acknowledgment ack) {
        if (!rateLimiter.tryConsume(notification.getTenantId())) {
            ack.nack(1000L);
            return;
        }
        String idKey = "notif:" + notification.getIdempotencyKey();
        Boolean first = redisTemplate.opsForValue().setIfAbsent(idKey, "1", Duration.ofHours(1));
        if (Boolean.FALSE.equals(first)) {
            ack.acknowledge();
            return;
        }
        dispatcher.dispatch(notification);
        ack.acknowledge();
    }
}
