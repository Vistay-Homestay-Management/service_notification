package com.vistay.notification.adapter.in.messaging;

import com.vistay.notification.application.DomainEventNotificationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka adapter for the explicitly registered domain-event inbox routes. */
@Component
public class DomainEventNotificationListener {
    private final DomainEventNotificationService notifications;

    public DomainEventNotificationListener(DomainEventNotificationService notifications) {
        this.notifications = notifications;
    }

    @KafkaListener(
        topicPattern = "${vistay.topics.domain-event-pattern:(vistay\\.(auth|booking|operations|finance|payment|system)\\..*)\\.v[0-9]+}",
        groupId = "${vistay.topics.domain-event-group:${spring.kafka.consumer.group-id:vistay-notification}-domain-inbox-v3}"
    )
    public void onDomainEvent(String rawEnvelope) {
        notifications.ingest(rawEnvelope);
    }
}
