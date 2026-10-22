package com.vistay.notification.adapter.in.messaging;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class ProfileProvisionedListener {
    public static final String EVENT_TYPE = "vistay.user.profile-provisioned";
    public static final String NOTIFICATION_CREATED = "vistay.notification.notification-created";
    private static final String CONSUMER_NAME = "notification-profile-welcome-v1";

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public ProfileProvisionedListener(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @KafkaListener(
        topics = "${vistay.topics.profile-provisioned:vistay.user.profile-provisioned.v1}",
        groupId = "${spring.kafka.consumer.group-id:vistay-notification}"
    )
    @Transactional
    public void onProfileProvisioned(String rawEnvelope) throws Exception {
        var envelope = json.readValue(rawEnvelope, EventEnvelope.class);
        if (!EVENT_TYPE.equals(envelope.type()) || envelope.version() != 1) {
            throw new IllegalArgumentException("Unsupported profile provisioning event");
        }
        if (jdbc.sql("""
            INSERT INTO processed_events(event_id, consumer_name)
            VALUES (:eventId, :consumerName)
            ON CONFLICT DO NOTHING
            """)
            .param("eventId", envelope.eventId())
            .param("consumerName", CONSUMER_NAME)
            .update() == 0) {
            return;
        }

        var accountId = UUID.fromString(requiredText(envelope.data(), "accountId"));
        var displayName = requiredText(envelope.data(), "displayName");
        var notificationId = jdbc.sql("""
            INSERT INTO notifications(
                recipient_account_type, recipient_user_id, notification_type,
                title, body, priority, source_service, source_event_id, status, data
            ) VALUES (
                'ADMIN', :accountId, 'BACKOFFICE_WELCOME',
                'Chào mừng đến ViStay', :body, 'HIGH', 'user', :sourceEventId, 'SENT',
                jsonb_build_object('securityNotice', true)
            )
            ON CONFLICT (source_service, source_event_id, notification_type,
                         recipient_account_type, recipient_user_id)
                WHERE source_event_id IS NOT NULL
            DO UPDATE SET updated_at = notifications.updated_at
            RETURNING id
            """)
            .param("accountId", accountId)
            .param("body", "Xin chào " + displayName
                + ". Tài khoản Backoffice đã sẵn sàng. Hãy đổi mật khẩu tạm thời và bật MFA.")
            .param("sourceEventId", envelope.eventId())
            .query(UUID.class)
            .single();

        var payload = json.writeValueAsString(new NotificationCreatedData(
            notificationId, accountId, "ADMIN", "BACKOFFICE_WELCOME"
        ));
        jdbc.sql("""
            INSERT INTO outbox_events(
                aggregate_type, aggregate_id, aggregate_version, event_type,
                event_version, payload, correlation_id, causation_id
            ) VALUES (
                'Notification', :notificationId, 0, :eventType,
                1, CAST(:payload AS jsonb), :correlationId, :causationId
            )
            """)
            .param("notificationId", notificationId)
            .param("eventType", NOTIFICATION_CREATED)
            .param("payload", payload)
            .param("correlationId", envelope.correlationId())
            .param("causationId", envelope.eventId().toString())
            .update();
    }

    private static String requiredText(JsonNode data, String field) {
        var node = data.get(field);
        if (node == null || node.asString().isBlank()) {
            throw new IllegalArgumentException("Missing event data field: " + field);
        }
        return node.asString();
    }

    public record EventEnvelope(UUID eventId, String type, int version, UUID aggregateId,
        long aggregateVersion, String correlationId, String causationId, OffsetDateTime occurredAt,
        JsonNode data) {}
    private record NotificationCreatedData(UUID notificationId, UUID accountId,
        String accountType, String notificationType) {}
}
