package com.vistay.notification.adapter.out.messaging;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class OutboxPublisher {
    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final long timeoutSeconds;

    public OutboxPublisher(JdbcClient jdbc, KafkaTemplate<String, String> kafka, ObjectMapper json,
        @Value("${vistay.outbox.send-timeout-seconds:10}") long timeoutSeconds) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.json = json;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Scheduled(fixedDelayString = "${vistay.outbox.poll-delay-ms:1000}")
    @Transactional
    public void publishPending() {
        for (var event : lockPending()) {
            try {
                var envelope = new EventEnvelope(event.id(), event.eventType(), event.eventVersion(),
                    event.aggregateId(), event.aggregateVersion(), event.correlationId(),
                    event.causationId(), event.occurredAt(), json.readTree(event.payload()));
                kafka.send(event.eventType() + ".v" + event.eventVersion(),
                    event.aggregateId().toString(), json.writeValueAsString(envelope))
                    .get(timeoutSeconds, TimeUnit.SECONDS);
                jdbc.sql("UPDATE outbox_events SET published_at = now() WHERE id = :id")
                    .param("id", event.id()).update();
            } catch (Exception failure) {
                jdbc.sql("""
                    UPDATE outbox_events
                    SET publish_attempts = publish_attempts + 1,
                        available_at = now() + LEAST((publish_attempts + 1) * interval '5 seconds', interval '5 minutes'),
                        last_error = :error
                    WHERE id = :id
                    """)
                    .param("id", event.id()).param("error", truncate(failure.getMessage())).update();
            }
        }
    }

    private List<OutboxRow> lockPending() {
        return jdbc.sql("""
            SELECT id, aggregate_id, aggregate_version, event_type, event_version,
                   payload::text AS payload, correlation_id, causation_id, occurred_at
            FROM outbox_events
            WHERE published_at IS NULL AND available_at <= now()
            ORDER BY occurred_at FOR UPDATE SKIP LOCKED LIMIT 50
            """).query(OutboxRow.class).list();
    }

    private static String truncate(String value) {
        var message = value == null ? "Unknown outbox publish failure" : value;
        return message.length() <= 2_000 ? message : message.substring(0, 2_000);
    }

    private record OutboxRow(UUID id, UUID aggregateId, long aggregateVersion, String eventType,
        int eventVersion, String payload, String correlationId, String causationId,
        OffsetDateTime occurredAt) {}
    private record EventEnvelope(UUID eventId, String type, int version, UUID aggregateId,
        long aggregateVersion, String correlationId, String causationId, OffsetDateTime occurredAt,
        JsonNode data) {}
}
