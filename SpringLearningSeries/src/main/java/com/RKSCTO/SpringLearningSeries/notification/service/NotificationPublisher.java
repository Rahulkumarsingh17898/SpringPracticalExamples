package com.RKSCTO.SpringLearningSeries.notification.service;

import com.RKSCTO.SpringLearningSeries.notification.model.Notification;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class NotificationPublisher {
    private final KafkaTemplate<String, Notification> kafkaTemplate;

    public NotificationPublisher(KafkaTemplate<String, Notification> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(Notification notification) {
        kafkaTemplate.send("notifications", notification.getTenantId(), notification);
    }
}
