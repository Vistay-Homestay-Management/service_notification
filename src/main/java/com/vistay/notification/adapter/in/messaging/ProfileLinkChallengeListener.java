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
public class ProfileLinkChallengeListener {
    public static final String EVENT_TYPE = "vistay.user.profile-link-challenge-issued";

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public ProfileLinkChallengeListener(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @KafkaListener(
        topics = "${vistay.topics.profile-link-challenge-issued:vistay.user.profile-link-challenge-issued.v1}",
        groupId = "${spring.kafka.consumer.group-id:vistay-notification}-profile-link-email-v1"
    )
    @Transactional
    public void onChallengeIssued(String rawEnvelope) throws Exception {
        var envelope = json.readValue(rawEnvelope, EventEnvelope.class);
        if (!EVENT_TYPE.equals(envelope.type()) || envelope.version() != 1) {
            throw new IllegalArgumentException("Unsupported profile link challenge event");
        }
        var data = envelope.data();
        var ciphertext = requiredText(data, "deliveryCiphertext");
        var keyVersion = requiredInt(data, "deliveryKeyVersion");
        if (!ciphertext.startsWith("vault:v" + keyVersion + ":")) {
            throw new IllegalArgumentException("Delivery ciphertext key version mismatch");
        }
        jdbc.sql("""
            INSERT INTO profile_link_email_deliveries(
                source_event_id, link_id, challenge_id, profile_type, masked_recipient,
                delivery_ciphertext, delivery_key_version, expires_at,
                correlation_id, causation_id
            ) VALUES (
                :sourceEventId, :linkId, :challengeId, :profileType, :maskedRecipient,
                :ciphertext, :keyVersion, :expiresAt,
                :correlationId, :causationId
            )
            ON CONFLICT (source_event_id) DO NOTHING
            """)
            .param("sourceEventId", envelope.eventId())
            .param("linkId", UUID.fromString(requiredText(data, "linkId")))
            .param("challengeId", UUID.fromString(requiredText(data, "challengeId")))
            .param("profileType", requiredText(data, "profileType"))
            .param("maskedRecipient", requiredText(data, "maskedRecipient"))
            .param("ciphertext", ciphertext)
            .param("keyVersion", keyVersion)
            .param("expiresAt", OffsetDateTime.parse(requiredText(data, "expiresAt")))
            .param("correlationId", envelope.correlationId())
            .param("causationId", envelope.eventId().toString())
            .update();
    }

    private static String requiredText(JsonNode data, String field) {
        var value = data == null ? null : data.get(field);
        if (value == null || value.asString().isBlank()) {
            throw new IllegalArgumentException("Missing event data field: " + field);
        }
        return value.asString();
    }

    private static int requiredInt(JsonNode data, String field) {
        var value = data == null ? null : data.get(field);
        if (value == null || !value.isInt() || value.asInt() <= 0) {
            throw new IllegalArgumentException("Invalid event data field: " + field);
        }
        return value.asInt();
    }

    public record EventEnvelope(UUID eventId, String type, int version, UUID aggregateId,
        long aggregateVersion, String correlationId, String causationId, OffsetDateTime occurredAt,
        JsonNode data) {}
}
