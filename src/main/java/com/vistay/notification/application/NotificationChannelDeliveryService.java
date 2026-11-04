package com.vistay.notification.application;

import com.vistay.notification.application.NotificationChannelDeliveryPolicy.ChannelIntent;
import com.vistay.notification.application.NotificationChannelDeliveryPolicy.DeliveryPlan;
import com.vistay.notification.application.NotificationChannelDeliveryPolicy.DeviceSnapshot;
import com.vistay.notification.application.NotificationChannelDeliveryPolicy.PolicySnapshot;
import com.vistay.notification.application.NotificationChannelDeliveryPolicy.PreferenceSnapshot;
import com.vistay.notification.application.NotificationEventRouteRegistry.EventRoute;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Freezes template/policy/preference revisions and delegates only encrypted snapshots. */
@Service
public class NotificationChannelDeliveryService {
    private final JdbcClient jdbc;
    private final ObjectMapper json;
    private final NotificationTemplateService templates;
    private final List<NotificationProviderPort> providers;
    private final Duration providerTimeout;

    public NotificationChannelDeliveryService(
            JdbcClient jdbc,
            ObjectMapper json,
            NotificationTemplateService templates,
            List<NotificationProviderPort> providers) {
        this(jdbc, json, templates, providers, Duration.ofSeconds(2));
    }

    @Autowired
    public NotificationChannelDeliveryService(
            JdbcClient jdbc,
            ObjectMapper json,
            NotificationTemplateService templates,
            List<NotificationProviderPort> providers,
            @Value("${vistay.notifications.provider-timeout:2s}") Duration providerTimeout) {
        this.jdbc = jdbc;
        this.json = json;
        this.templates = templates;
        this.providers = providers == null ? List.of() : List.copyOf(providers);
        this.providerTimeout = providerTimeout;
    }

    /**
     * Evaluates all gates before a delivery row is frozen. Optional push can be
     * omitted; required in-app/email or a missing published template fails.
     */
    @Transactional(readOnly = true)
    public DeliveryEvaluation evaluate(
            EventRoute route,
            PolicySnapshot policy,
            PreferenceSnapshot preferences,
            DeviceSnapshot device,
            java.time.Instant now) {
        DeliveryPlan plan = NotificationChannelDeliveryPolicy.evaluate(
            route, policy, preferences, device, now);
        List<SelectedTemplate> selected = new ArrayList<>();
        for (ChannelIntent intent : plan.intents()) {
            var template = templates.findPublished(route.templateKey(), intent.channel(), "vi");
            if (template.isEmpty()) {
                if (intent.required()) {
                    throw new NotificationCommandException("TEMPLATE_NOT_PUBLISHED",
                        "Required notification template is not published");
                }
                continue;
            }
            selected.add(new SelectedTemplate(intent, route.templateKey(), template.get().id(),
                template.get().revision(), template.get().policyRevision()));
        }
        return new DeliveryEvaluation(plan, List.copyOf(selected));
    }

    @Transactional(readOnly = true)
    public PolicySnapshot resolvePolicy(EventRoute route, boolean globalPushEnabled) {
        PolicyRow row = findPolicy(route.routeKey());
        if (row == null) {
            return PolicySnapshot.fromRoute(route, globalPushEnabled);
        }
        List<String> required = channels(row.requiredChannelsJson());
        List<String> optional = channels(row.optionalChannelsJson());
        List<String> allowedTypes = channels(row.allowedNotificationTypesJson());
        boolean routeAllowsPush = allowedTypes.isEmpty() || allowedTypes.contains(route.notificationType());
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("policyKey", row.policyKey());
        snapshot.put("revision", row.revision());
        snapshot.put("pushEnabled", globalPushEnabled && row.pushEnabled());
        snapshot.put("requiredChannels", required);
        snapshot.put("optionalChannels", optional);
        return new PolicySnapshot(row.policyKey(), row.revision(), required, optional,
            globalPushEnabled && row.pushEnabled(), routeAllowsPush, snapshot);
    }

    /** Persist the evaluated snapshot atomically with the notification transaction. */
    @Transactional
    public DeliveryEvaluation freeze(
            UUID notificationId,
            EventRoute route,
            PolicySnapshot policy,
            PreferenceSnapshot preferences,
            DeviceSnapshot device,
            java.time.Instant now) {
        DeliveryEvaluation evaluation = evaluate(route, policy, preferences, device, now);
        freeze(notificationId, evaluation);
        return evaluation;
    }

    @Transactional
    public void freeze(UUID notificationId, DeliveryEvaluation evaluation) {
        for (SelectedTemplate selected : evaluation.templates()) {
            ChannelIntent intent = selected.intent();
            jdbc.sql("""
                    UPDATE notification_deliveries
                    SET required = :required,
                        template_id = :templateId,
                        template_key = :templateKey,
                        template_revision = :templateRevision,
                        policy_revision = :policyRevision,
                        preference_revision = :preferenceRevision,
                        feature_policy_snapshot = CAST(:policySnapshot AS jsonb),
                        device_reference = :deviceReference,
                        quiet_hours_deferred_until = :deferredUntil,
                        next_attempt_at = COALESCE(:deferredUntil, now()),
                        updated_at = now()
                    WHERE notification_id = :notificationId AND channel = :channel
                    """)
                .param("required", intent.required())
                .param("templateId", selected.templateId())
                .param("templateKey", selected.templateKey())
                .param("templateRevision", selected.templateRevision())
                .param("policyRevision", evaluation.plan().policyRevision())
                .param("preferenceRevision", evaluation.plan().preferenceRevision())
                .param("policySnapshot", jsonObject(evaluation.plan().policySnapshot()))
                .param("deviceReference", evaluation.plan().deviceReference())
                .param("deferredUntil", intent.deferredUntil() == null
                    ? null : OffsetDateTime.ofInstant(intent.deferredUntil(), ZoneOffset.UTC))
                .param("notificationId", notificationId)
                .param("channel", intent.channel())
                .update();
        }
    }

    /**
     * Sends a bounded batch through registered provider ports. The worker only
     * handles encrypted destination snapshots and stable metadata; it never
     * logs or reconstructs a raw destination.
     */
    @Transactional
    public int processPending(int limit) {
        int boundedLimit = Math.max(1, Math.min(limit, 100));
        List<DeliveryRow> rows = jdbc.sql("""
                SELECT d.id, d.channel, d.provider_idempotency_key, d.destination_masked,
                       d.destination_ciphertext, d.attempt_count, d.maximum_attempts,
                       n.title, n.body
                FROM notification_deliveries d
                JOIN notifications n ON n.id = d.notification_id
                WHERE d.status = 'PENDING'
                  AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= now())
                  AND (d.quiet_hours_deferred_until IS NULL OR d.quiet_hours_deferred_until <= now())
                ORDER BY d.created_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """)
            .param("limit", boundedLimit)
            .query(DeliveryRow.class)
            .list();
        int processed = 0;
        for (DeliveryRow row : rows) {
            if (claim(row)) {
                dispatch(row);
                processed++;
            }
        }
        return processed;
    }

    private boolean claim(DeliveryRow row) {
        return jdbc.sql("""
                UPDATE notification_deliveries
                SET status = 'SENDING', attempt_count = attempt_count + 1, updated_at = now()
                WHERE id = :id AND status = 'PENDING' AND attempt_count < maximum_attempts
                """)
            .param("id", row.id())
            .update() == 1;
    }

    private void dispatch(DeliveryRow row) {
        if (NotificationChannelDeliveryPolicy.IN_APP.equals(row.channel())) {
            complete(row, null, "SENT", null, null, null);
            return;
        }
        if (row.destinationCiphertext() == null || row.destinationCiphertext().length == 0) {
            fail(row, "MISSING_CONTACT", "INVALID_CONTACT", null);
            return;
        }
        NotificationProviderPort provider = providers.stream()
            .filter(candidate -> row.channel().equals(candidate.channel()))
            .findFirst()
            .orElse(null);
        if (provider == null) {
            fail(row, "PROVIDER_UNAVAILABLE", "PROVIDER_OUTAGE", null);
            return;
        }
        try {
            NotificationProviderPort.SendResult result = provider.send(new NotificationProviderPort.SendRequest(
                row.id(), row.channel(), row.providerIdempotencyKey(), row.destinationMasked(),
                row.destinationCiphertext(), row.title(), row.body(), providerTimeout));
            if (result.outcome() == NotificationProviderPort.Outcome.ACCEPTED) {
                complete(row, provider.provider(), "ACCEPTED", result.providerMessageId(), null, null);
            } else {
                fail(row, result.errorCode(), result.errorClass(), result.retryAt());
            }
        } catch (RuntimeException providerFailure) {
            // Provider exceptions are intentionally collapsed to a stable code.
            fail(row, "PROVIDER_UNAVAILABLE", "PROVIDER_OUTAGE", null);
        }
    }

    private void complete(
            DeliveryRow row,
            String provider,
            String status,
            String providerMessageId,
            String errorCode,
            String errorClass) {
        jdbc.sql("""
                UPDATE notification_deliveries
                SET status = :status, provider = COALESCE(:provider, provider),
                    provider_message_id = :providerMessageId,
                    accepted_at = COALESCE(accepted_at, now()), sent_at = COALESCE(sent_at, now()),
                    last_error_code = :errorCode, error_class = :errorClass, updated_at = now()
                WHERE id = :id
                """)
            .param("status", status)
            .param("provider", provider)
            .param("providerMessageId", providerMessageId)
            .param("errorCode", errorCode)
            .param("errorClass", errorClass)
            .param("id", row.id())
            .update();
        attempt(row, status, null, null);
    }

    private void fail(DeliveryRow row, String errorCode, String errorClass, OffsetDateTime retryAt) {
        jdbc.sql("""
                UPDATE notification_deliveries
                SET status = 'FAILED', failed_at = now(), next_attempt_at = :retryAt,
                    last_error_code = :errorCode, error_class = :errorClass, updated_at = now()
                WHERE id = :id
                """)
            .param("retryAt", retryAt)
            .param("errorCode", errorCode)
            .param("errorClass", errorClass)
            .param("id", row.id())
            .update();
        attempt(row, "FAILED", errorCode, errorClass);
    }

    private void attempt(DeliveryRow row, String result, String errorCode, String errorClass) {
        jdbc.sql("""
                INSERT INTO delivery_attempts(
                    delivery_id, attempt_number, result, error_code, error_message
                ) VALUES (:deliveryId, :attemptNumber, :result, :errorCode, :errorMessage)
                ON CONFLICT (delivery_id, attempt_number) DO NOTHING
                """)
            .param("deliveryId", row.id())
            .param("attemptNumber", row.attemptCount() + 1)
            .param("result", "SENT".equals(result) || "ACCEPTED".equals(result)
                ? "ACCEPTED" : "FAILED")
            .param("errorCode", errorCode)
            .param("errorMessage", errorClass)
            .update();
    }

    private String jsonObject(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception serializationFailure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Policy snapshot cannot be serialized");
        }
    }

    private PolicyRow findPolicy(String routeKey) {
        PolicyRow routePolicy = policy(routeKey);
        return routePolicy == null ? policy("notification.default") : routePolicy;
    }

    private PolicyRow policy(String policyKey) {
        return jdbc.sql("""
                SELECT policy_key, revision, required_channels::text AS required_channels_json,
                       optional_channels::text AS optional_channels_json, push_enabled,
                       allowed_notification_types::text AS allowed_notification_types_json
                FROM notification_policies
                WHERE policy_key = :policyKey
                ORDER BY revision DESC
                LIMIT 1
                """)
            .param("policyKey", policyKey)
            .query(PolicyRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private List<String> channels(String raw) {
        try {
            JsonNode node = json.readTree(raw == null ? "[]" : raw);
            if (!node.isArray()) {
                throw new IllegalArgumentException("policy channels must be an array");
            }
            List<String> values = new ArrayList<>();
            node.forEach(value -> values.add(value.asText()));
            return List.copyOf(values);
        } catch (Exception invalidPolicy) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Notification policy is invalid");
        }
    }

    public record DeliveryEvaluation(DeliveryPlan plan, List<SelectedTemplate> templates) {}

    public record SelectedTemplate(
            ChannelIntent intent,
            String templateKey,
            UUID templateId,
            int templateRevision,
            int policyRevision) {}

    private record DeliveryRow(
            UUID id,
            String channel,
            String providerIdempotencyKey,
            String destinationMasked,
            byte[] destinationCiphertext,
            int attemptCount,
            int maximumAttempts,
            String title,
            String body) {}

    private record PolicyRow(
            String policyKey,
            int revision,
            String requiredChannelsJson,
            String optionalChannelsJson,
            boolean pushEnabled,
            String allowedNotificationTypesJson) {}
}
