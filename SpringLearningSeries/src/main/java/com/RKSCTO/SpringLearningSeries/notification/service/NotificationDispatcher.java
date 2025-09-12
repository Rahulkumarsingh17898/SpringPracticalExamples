package com.RKSCTO.SpringLearningSeries.notification.service;

import com.RKSCTO.SpringLearningSeries.notification.model.Notification;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

@Component
public class NotificationDispatcher {
    private final SimpMessagingTemplate messagingTemplate;
    private final Map<String, Sinks.Many<Notification>> sinkMap = new ConcurrentHashMap<>();

    public NotificationDispatcher(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void dispatch(Notification notification) {
        Sinks.Many<Notification> sink = sinkMap.computeIfAbsent(notification.getTenantId(), tenant -> {
            Sinks.Many<Notification> s = Sinks.many().multicast().onBackpressureBuffer(100);
            s.asFlux().subscribe(n -> messagingTemplate.convertAndSend("/topic/tenant." + tenant, n));
            return s;
        });
        sink.tryEmitNext(notification);
    }
}
