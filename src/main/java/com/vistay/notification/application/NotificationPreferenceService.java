package com.vistay.notification.application;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** Owns recipient-self preference revisions and mandatory-channel invariants. */
@Service
public class NotificationPreferenceService {
    private static final Pattern NOTIFICATION_TYPE =
        Pattern.compile("^[a-z][a-z0-9]*(?:[.-][a-z0-9]+|[*])*$|^\\*$");
    private static final List<String> CHANNELS = List.of("IN_APP", "EMAIL", "PUSH");

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public NotificationPreferenceService(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional(readOnly = true)
    public PreferenceConnection get(String accountType, UUID accountId) {
        List<Preference> preferences = jdbc.sql("""
                SELECT id, notification_type, channel, enabled, revision,
                       quiet_hours_start, quiet_hours_end, time_zone_id, updated_at
                FROM notification_preferences
                WHERE recipient_account_type = :accountType
                  AND recipient_user_id = :accountId
                ORDER BY notification_type, channel
                """)
            .param("accountType", accountType)
            .param("accountId", accountId)
            .query(PreferenceRow.class)
            .list()
            .stream()
            .map(this::map)
            .toList();
        int revision = preferences.stream().mapToInt(Preference::revision).max().orElse(1);
        return new PreferenceConnection(preferences, revision);
    }

    @Transactional
    public PreferenceConnection update(UpdateCommand command) {
        validate(command);
        String fingerprint = fingerprint(command);
        IdempotencyRow previous = findIdempotency(command.idempotencyKey());
        if (previous != null) {
            ensureSameRequest(previous, fingerprint);
            return get(command.accountType(), command.accountId());
        }

        List<PreferenceUpdate> updates = command.updates();
        for (PreferenceUpdate update : updates) {
            PreferenceRow current = find(command.accountType(), command.accountId(),
                update.notificationType(), update.channel());
            if (current == null) {
                if (update.expectedVersion() != 0) {
                    throw new NotificationCommandException("STALE_REVISION",
                        "Preference revision does not match the expected version");
                }
                insert(command, update);
            } else {
                if (current.revision() != update.expectedVersion()) {
                    throw new NotificationCommandException("STALE_REVISION",
                        "Preference revision does not match the expected version");
                }
                int changed = jdbc.sql("""
                        UPDATE notification_preferences
                        SET enabled = :enabled, quiet_hours_start = :quietStart,
                            quiet_hours_end = :quietEnd, time_zone_id = :timeZoneId,
                            revision = revision + 1, feature_policy_snapshot = CAST(:policy AS jsonb),
                            updated_at = now()
                        WHERE id = :id AND revision = :expectedVersion
                        """)
                    .param("enabled", update.enabled())
                    .param("quietStart", update.quietHoursStart())
                    .param("quietEnd", update.quietHoursEnd())
                    .param("timeZoneId", update.timeZoneId())
                    .param("policy", policySnapshot(update))
                    .param("id", current.id())
                    .param("expectedVersion", update.expectedVersion())
                    .update();
                if (changed != 1) {
                    throw new NotificationCommandException("CONFLICT",
                        "Preference was changed concurrently");
                }
            }
        }

        PreferenceConnection result = get(command.accountType(), command.accountId());
        UUID aggregateId = command.accountId();
        jdbc.sql("""
                INSERT INTO audit_events(
                    actor_admin_user_id, action, aggregate_type, aggregate_id, changes
                ) VALUES (:actorId, 'NOTIFICATION_PREFERENCES_UPDATED',
                    'NotificationPreference', :aggregateId, CAST(:changes AS jsonb))
                """)
            .param("actorId", command.actorAccountId())
            .param("aggregateId", aggregateId)
            .param("changes", jsonObject(Map.of(
                "accountType", command.accountType(),
                "revision", result.revision(),
                "idempotencyKey", command.idempotencyKey())))
            .update();
        jdbc.sql("""
                INSERT INTO outbox_events(
                    aggregate_type, aggregate_id, aggregate_version, event_type,
                    event_version, payload, causation_id
                ) VALUES ('NotificationPreference', :aggregateId, :revision,
                    'vistay.notification.preference-updated', 1,
                    CAST(:payload AS jsonb), :causationId)
                """)
            .param("aggregateId", aggregateId)
            .param("revision", result.revision())
            .param("payload", jsonObject(Map.of(
                "recipientAccountType", command.accountType(),
                "recipientAccountId", command.accountId(),
                "revision", result.revision())))
            .param("causationId", command.idempotencyKey())
            .update();
        saveIdempotency(command, fingerprint, result);
        return result;
    }

    private void insert(UpdateCommand command, PreferenceUpdate update) {
        jdbc.sql("""
                INSERT INTO notification_preferences(
                    recipient_account_type, recipient_user_id, notification_type,
                    channel, enabled, quiet_hours_start, quiet_hours_end, time_zone_id,
                    revision, feature_policy_snapshot
                ) VALUES (:accountType, :accountId, :notificationType, :channel,
                    :enabled, :quietStart, :quietEnd, :timeZoneId, 1,
                    CAST(:policy AS jsonb))
                ON CONFLICT (recipient_account_type, recipient_user_id, notification_type, channel)
                DO NOTHING
                """)
            .param("accountType", command.accountType())
            .param("accountId", command.accountId())
            .param("notificationType", update.notificationType())
            .param("channel", update.channel())
            .param("enabled", update.enabled())
            .param("quietStart", update.quietHoursStart())
            .param("quietEnd", update.quietHoursEnd())
            .param("timeZoneId", update.timeZoneId())
            .param("policy", policySnapshot(update))
            .update();
    }

    private PreferenceRow find(String accountType, UUID accountId, String type, String channel) {
        return jdbc.sql("""
                SELECT id, notification_type, channel, enabled, revision,
                       quiet_hours_start, quiet_hours_end, time_zone_id, updated_at
                FROM notification_preferences
                WHERE recipient_account_type = :accountType AND recipient_user_id = :accountId
                  AND notification_type = :notificationType AND channel = :channel
                FOR UPDATE
                """)
            .param("accountType", accountType)
            .param("accountId", accountId)
            .param("notificationType", type)
            .param("channel", channel)
            .query(PreferenceRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private IdempotencyRow findIdempotency(String key) {
        return jdbc.sql("""
                SELECT request_fingerprint, result
                FROM notification_command_idempotency
                WHERE operation = 'preference.update' AND idempotency_key = :idempotencyKey
                """)
            .param("idempotencyKey", key)
            .query(IdempotencyRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private void saveIdempotency(UpdateCommand command, String fingerprint, PreferenceConnection result) {
        jdbc.sql("""
                INSERT INTO notification_command_idempotency(
                    operation, idempotency_key, actor_account_type, actor_user_id,
                    request_fingerprint, result
                ) VALUES ('preference.update', :idempotencyKey, :actorType, :actorId,
                    :fingerprint, CAST(:result AS jsonb))
                ON CONFLICT (operation, idempotency_key) DO NOTHING
                """)
            .param("idempotencyKey", command.idempotencyKey())
            .param("actorType", command.actorAccountType())
            .param("actorId", command.actorAccountId())
            .param("fingerprint", fingerprint)
            .param("result", jsonObject(Map.of("revision", result.revision())))
            .update();
    }

    private void validate(UpdateCommand command) {
        if (command == null || command.accountId() == null || command.updates() == null
                || command.updates().isEmpty() || command.updates().size() > 50
                || blank(command.accountType()) || !List.of("CUSTOMER", "ADMIN", "HOST")
                    .contains(command.accountType()) || blank(command.idempotencyKey())) {
            throw new NotificationCommandException("VALIDATION_ERROR", "Notification preference input is invalid");
        }
        long unique = command.updates().stream()
            .map(update -> update.notificationType() + "|" + update.channel())
            .distinct().count();
        if (unique != command.updates().size()) {
            throw new NotificationCommandException("VALIDATION_ERROR", "Duplicate preference update");
        }
        for (PreferenceUpdate update : command.updates()) {
            if (blank(update.notificationType()) || !NOTIFICATION_TYPE.matcher(update.notificationType()).matches()
                    || !CHANNELS.contains(update.channel()) || update.expectedVersion() < 0
                    || (isRequired(update.channel()) && !update.enabled())
                    || (update.quietHoursStart() == null) != (update.quietHoursEnd() == null)
                    || blank(update.timeZoneId())) {
                throw new NotificationCommandException(
                    isRequired(update.channel()) && !update.enabled()
                        ? "MANDATORY_CHANNEL_DISABLE" : "VALIDATION_ERROR",
                    isRequired(update.channel()) && !update.enabled()
                        ? "Required notification channels cannot be disabled"
                        : "Notification preference input is invalid");
            }
            try {
                ZoneId.of(update.timeZoneId());
            } catch (Exception invalidZone) {
                throw new NotificationCommandException("INVALID_TIME_ZONE", "Notification time zone is invalid");
            }
        }
    }

    private static boolean isRequired(String channel) {
        return NotificationChannelDeliveryPolicy.IN_APP.equals(channel)
            || NotificationChannelDeliveryPolicy.EMAIL.equals(channel);
    }

    private String policySnapshot(PreferenceUpdate update) {
        return jsonObject(Map.of(
            "channel", update.channel(),
            "enabled", update.enabled(),
            "required", isRequired(update.channel()),
            "timeZoneId", update.timeZoneId()));
    }

    private String jsonObject(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception serializationFailure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Preference metadata cannot be serialized");
        }
    }

    private Preference map(PreferenceRow row) {
        return new Preference(row.id(), row.notificationType(), row.channel(), row.enabled(),
            isRequired(row.channel()), row.quietHoursStart(), row.quietHoursEnd(), row.timeZoneId(),
            row.revision(), row.updatedAt());
    }

    private static String fingerprint(UpdateCommand command) {
        List<String> values = new ArrayList<>();
        values.add(command.accountType());
        values.add(command.accountId().toString());
        for (PreferenceUpdate update : command.updates()) {
            values.add(update.notificationType());
            values.add(update.channel());
            values.add(Boolean.toString(update.enabled()));
            values.add(String.valueOf(update.quietHoursStart()));
            values.add(String.valueOf(update.quietHoursEnd()));
            values.add(update.timeZoneId());
            values.add(Integer.toString(update.expectedVersion()));
        }
        return DomainEventNotificationService.fingerprint(values.toArray(String[]::new));
    }

    private static void ensureSameRequest(IdempotencyRow previous, String fingerprint) {
        if (!fingerprint.equals(previous.requestFingerprint())) {
            throw new NotificationCommandException("IDEMPOTENCY_KEY_REUSED",
                "Idempotency key was already used for another request");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    public record UpdateCommand(
            String accountType,
            UUID accountId,
            List<PreferenceUpdate> updates,
            String idempotencyKey,
            String actorAccountType,
            UUID actorAccountId) {}

    public record PreferenceUpdate(
            String notificationType,
            String channel,
            boolean enabled,
            LocalTime quietHoursStart,
            LocalTime quietHoursEnd,
            String timeZoneId,
            int expectedVersion) {}

    public record PreferenceConnection(List<Preference> nodes, int revision) {}

    public record Preference(
            UUID id,
            String notificationType,
            String channel,
            boolean enabled,
            boolean required,
            LocalTime quietHoursStart,
            LocalTime quietHoursEnd,
            String timeZoneId,
            int revision,
            OffsetDateTime updatedAt) {}

    private record PreferenceRow(
            UUID id,
            String notificationType,
            String channel,
            boolean enabled,
            int revision,
            LocalTime quietHoursStart,
            LocalTime quietHoursEnd,
            String timeZoneId,
            OffsetDateTime updatedAt) {}

    private record IdempotencyRow(String requestFingerprint, String resultJson) {}
}
