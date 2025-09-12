package com.RKSCTO.SpringLearningSeries.notification.controller;

import com.RKSCTO.SpringLearningSeries.notification.model.Notification;
import com.RKSCTO.SpringLearningSeries.notification.service.NotificationPublisher;
import java.util.Map;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NotificationController {
    private final NotificationPublisher publisher;

    public NotificationController(NotificationPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/notify/{tenant}")
    public void notifyTenant(@PathVariable String tenant, @RequestBody Map<String, String> payload) {
        Notification notification = new Notification();
        notification.setTenantId(tenant);
        notification.setMessage(payload.get("message"));
        notification.setIdempotencyKey(UUID.randomUUID().toString());
        publisher.publish(notification);
    }
}
