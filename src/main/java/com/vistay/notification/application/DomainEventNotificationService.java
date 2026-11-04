package com.vistay.notification.application;

import com.vistay.notification.application.NotificationEventRouteRegistry.EventRoute;
import com.vistay.notification.application.NotificationRecipientResolver.RecipientResolutionException;
import com.vistay.notification.application.NotificationRecipientResolver.ResolvedRecipient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.HexFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Atomically turns an allow-listed domain event into an inbox notification.
 * No source payload or raw contact value is persisted by this service.
 */
@Service
public class DomainEventNotificationService {
    public static final String CONSUMER_NAME = "notification-domain-inbox-v3";
    public static final String NOTIFICATION_CREATED = "vistay.notification.notification-created";

    private static final String[] REFERENCE_FIELDS = {
        "bookingId", "paymentIntentId", "financePaymentRequestId", "inventoryBlockId",
        "publicationId", "propertyId", "taskId", "caseId", "incidentId", "referenceId"
    };
    private static final String[] STATUS_FIELDS = {
        "status", "action", "matchStatus", "reasonCode"
    };

    private final JdbcClient jdbc;
    private final ObjectMapper json;
    private final NotificationEventRouteRegistry routes;
    private final NotificationRecipientResolver recipients;
    private final boolean pushFeatureEnabled;

    public DomainEventNotificationService(
            JdbcClient jdbc,
            ObjectMapper json,
            NotificationEventRouteRegistry routes,
            NotificationRecipientResolver recipients) {
        this(jdbc, json, routes, recipients, false);
    }

    @Autowired
    public DomainEventNotificationService(
            JdbcClient jdbc,
            ObjectMapper json,
            NotificationEventRouteRegistry routes,
            NotificationRecipientResolver recipients,
            @Value("${vistay.notifications.push-enabled:false}") boolean pushFeatureEnabled) {
        this.jdbc = jdbc;
        this.json = json;
        this.routes = routes;
        this.recipients = recipients;
        this.pushFeatureEnabled = pushFeatureEnabled;
    }

    /** Consumes one Kafka envelope and returns its durable outcome. */
    @Transactional
    public IngestResult ingest(String rawEnvelope) {
        String payloadFingerprint = fingerprint(rawEnvelope == null ? "" : rawEnvelope);
        DomainEventEnvelope envelope;
        try {
            envelope = parseEnvelope(rawEnvelope);
        } catch (IntakeException invalid) {
            return quarantine(null, null, null, payloadFingerprint, invalid.code(),
                metadata(invalid.code(), null));
        }
        payloadFingerprint = canonicalPayloadFingerprint(rawEnvelope);

        EventRoute route = routes.find(envelope.type(), envelope.version());
        if (route == null) {
            return quarantine(envelope, null, null, payloadFingerprint,
                "UNSUPPORTED_EVENT_ROUTE", metadata("UNSUPPORTED_EVENT_ROUTE", envelope));
        }

        IntakeRow eventReplay = findByEventRoute(envelope, route);
        if (eventReplay != null) {
            if (payloadFingerprint.equals(eventReplay.payloadFingerprint())) {
                return replayResult(eventReplay);
            }
            return quarantine(envelope, eventReplay.id(), null, payloadFingerprint,
                "CONFLICTING_REPLAY", metadata("CONFLICTING_REPLAY", envelope));
        }

        try {
            validateData(envelope.data(), route);
            ResolvedRecipient recipient = recipients.resolve(envelope.data(), route);
            return createDurableNotification(envelope, route, recipient, payloadFingerprint);
        } catch (IntakeException | RecipientResolutionException rejected) {
            String code = rejected instanceof IntakeException intake
                ? intake.code() : ((RecipientResolutionException) rejected).code();
            return quarantine(envelope, null, route, payloadFingerprint, code,
                metadata(code, envelope));
        }
    }

    private IngestResult createDurableNotification(
            DomainEventEnvelope envelope,
            EventRoute route,
            ResolvedRecipient recipient,
            String payloadFingerprint) {
        String recipientType = recipient.accountType().name();
        String recipientId = recipient.accountId().toString();
        String deduplicationKey = fingerprint(
            envelope.eventId().toString(), route.routeKey(), recipientType, recipientId);
        String businessKey = fingerprint(
            route.routeKey(), recipientType, recipientId,
            envelope.aggregateId().toString(), Long.toString(envelope.aggregateVersion()));
        Projection projection = project(envelope, route);

        IntakeRow businessReplay = findByBusinessKey(businessKey);
        if (businessReplay != null) {
            return quarantine(envelope, businessReplay.id(), route, payloadFingerprint,
                "CONFLICTING_BUSINESS_REPLAY", metadata("CONFLICTING_BUSINESS_REPLAY", envelope));
        }

        int claimed = jdbc.sql("""
                INSERT INTO notification_intake(
                    consumer_name, event_id, event_type, event_version,
                    aggregate_id, aggregate_version, route_key, route_version,
                    recipient_account_type, recipient_user_id,
                    recipient_resolution_revision, payload_fingerprint,
                    deduplication_key, business_deduplication_key, state, safe_metadata
                ) VALUES (
                    :consumerName, :eventId, :eventType, :eventVersion,
                    :aggregateId, :aggregateVersion, :routeKey, :routeVersion,
                    :recipientAccountType, :recipientUserId,
                    :resolutionRevision, :payloadFingerprint,
                    :deduplicationKey, :businessKey, 'RECEIVED',
                    CAST(:safeMetadata AS jsonb)
                )
                ON CONFLICT DO NOTHING
                """)
            .param("consumerName", CONSUMER_NAME)
            .param("eventId", envelope.eventId())
            .param("eventType", envelope.type())
            .param("eventVersion", envelope.version())
            .param("aggregateId", envelope.aggregateId())
            .param("aggregateVersion", envelope.aggregateVersion())
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("recipientAccountType", recipientType)
            .param("recipientUserId", recipient.accountId())
            .param("resolutionRevision", recipient.resolutionRevision())
            .param("payloadFingerprint", payloadFingerprint)
            .param("deduplicationKey", deduplicationKey)
            .param("businessKey", businessKey)
            .param("safeMetadata", metadataJson(metadata("RECEIVED", envelope)))
            .update();

        IntakeRow intake = findByEventRoute(envelope, route);
        if (intake == null) {
            throw new IllegalStateException("Notification intake claim disappeared");
        }
        if (claimed == 0) {
            if (payloadFingerprint.equals(intake.payloadFingerprint())) {
                return replayResult(intake);
            }
            return quarantine(envelope, intake.id(), route, payloadFingerprint,
                "CONFLICTING_REPLAY", metadata("CONFLICTING_REPLAY", envelope));
        }

        updateIntake(intake.id(), "VALIDATED", null, null);
        updateIntake(intake.id(), "RESOLVED", recipient.resolutionRevision(), null);

        NotificationRow notification = jdbc.sql("""
                INSERT INTO notifications(
                    recipient_account_type, recipient_user_id, notification_type,
                    title, body, priority, source_service, source_event_id,
                    source_event_type, source_event_version, source_aggregate_id,
                    source_aggregate_version, source_correlation_id, route_key,
                    route_version, category, template_key, template_revision,
                    policy_revision, recipient_resolution_revision,
                    render_input_fingerprint, notification_revision, status, data
                ) VALUES (
                    :recipientAccountType, :recipientUserId, :notificationType,
                    :title, :body, :priority, :sourceService, :sourceEventId,
                    :sourceEventType, :sourceEventVersion, :sourceAggregateId,
                    :sourceAggregateVersion, :sourceCorrelationId, :routeKey,
                    :routeVersion, :category, :templateKey, :templateRevision,
                    :policyRevision, :resolutionRevision,
                    :renderInputFingerprint, 1, 'PENDING', CAST(:data AS jsonb)
                )
                RETURNING id, event_sequence, created_at
                """)
            .param("recipientAccountType", recipientType)
            .param("recipientUserId", recipient.accountId())
            .param("notificationType", route.notificationType())
            .param("title", projection.title())
            .param("body", projection.body())
            .param("priority", route.priority())
            .param("sourceService", sourceService(envelope.type()))
            .param("sourceEventId", envelope.eventId())
            .param("sourceEventType", envelope.type())
            .param("sourceEventVersion", envelope.version())
            .param("sourceAggregateId", envelope.aggregateId())
            .param("sourceAggregateVersion", envelope.aggregateVersion())
            .param("sourceCorrelationId", envelope.correlationId().toString())
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("category", route.category())
            .param("templateKey", route.templateKey())
            .param("templateRevision", 1)
            .param("policyRevision", 1)
            .param("resolutionRevision", recipient.resolutionRevision())
            .param("renderInputFingerprint", projection.renderInputFingerprint())
            .param("data", projection.dataJson())
            .query(NotificationRow.class)
            .single();

        jdbc.sql("""
                UPDATE notifications
                SET inbox_revision = event_sequence, updated_at = now()
                WHERE id = :notificationId
                """)
            .param("notificationId", notification.id())
            .update();

        jdbc.sql("""
                INSERT INTO notification_recipient_snapshots(
                    notification_id, recipient_account_type, recipient_user_id,
                    profile_id, authorization_version, resolution_revision,
                    contact_snapshot_revision, snapshot_fingerprint
                ) VALUES (
                    :notificationId, :recipientAccountType, :recipientUserId,
                    :profileId, :authorizationVersion, :resolutionRevision,
                    :contactSnapshotRevision, :snapshotFingerprint
                )
                """)
            .param("notificationId", notification.id())
            .param("recipientAccountType", recipientType)
            .param("recipientUserId", recipient.accountId())
            .param("profileId", recipient.profileId())
            .param("authorizationVersion", recipient.authorizationVersion())
            .param("resolutionRevision", recipient.resolutionRevision())
            .param("contactSnapshotRevision", recipient.contactSnapshotRevision())
            .param("snapshotFingerprint", recipient.snapshotFingerprint())
            .update();

        List<String> optionalChannels = pushFeatureEnabled ? route.optionalChannels() : List.of();
        List<String> channels = new ArrayList<>(route.requiredChannels());
        channels.addAll(optionalChannels);
        for (String channel : channels) {
            insertDelivery(notification.id(), recipient, route, channel,
                route.requiredChannels().contains(channel));
        }

        jdbc.sql("""
                INSERT INTO audit_events(
                    action, aggregate_type, aggregate_id, changes, correlation_id
                ) VALUES (
                    'DOMAIN_EVENT_NOTIFICATION_CREATED', 'Notification', :notificationId,
                    CAST(:changes AS jsonb), :correlationId
                )
                """)
            .param("notificationId", notification.id())
            .param("changes", metadataJson(metadata("CREATED", envelope, route)))
            .param("correlationId", envelope.correlationId().toString())
            .update();

        String outboxPayload = notificationCreatedPayload(
            envelope, route, recipient, notification, channels, optionalChannels);
        jdbc.sql("""
                INSERT INTO outbox_events(
                    aggregate_type, aggregate_id, aggregate_version, event_type,
                    event_version, payload, correlation_id, causation_id
                ) VALUES (
                    'Notification', :notificationId, :aggregateVersion, :eventType,
                    2, CAST(:payload AS jsonb), :correlationId, :causationId
                )
                """)
            .param("notificationId", notification.id())
            .param("aggregateVersion", notification.eventSequence())
            .param("eventType", NOTIFICATION_CREATED)
            .param("payload", outboxPayload)
            .param("correlationId", envelope.correlationId().toString())
            .param("causationId", envelope.eventId().toString())
            .update();

        updateIntake(intake.id(), "CREATED", recipient.resolutionRevision(), notification.id());
        return new IngestResult(Outcome.CREATED, "CREATED", notification.id());
    }

    private void insertDelivery(
            UUID notificationId,
            ResolvedRecipient recipient,
            EventRoute route,
            String channel,
            boolean required) {
        String providerKey = fingerprint(notificationId.toString(), channel, "1");
        jdbc.sql("""
                INSERT INTO notification_deliveries(
                    notification_id, channel, required, template_revision,
                    policy_revision, delivery_revision, provider_idempotency_key,
                    contact_snapshot_revision, destination_resolution_state, status
                ) VALUES (
                    :notificationId, :channel, :required, 1, 1, 1,
                    :providerIdempotencyKey, :contactSnapshotRevision, 'PENDING', 'PENDING'
                )
                ON CONFLICT (notification_id, channel) DO NOTHING
                """)
            .param("notificationId", notificationId)
            .param("channel", channel)
            .param("required", required)
            .param("providerIdempotencyKey", providerKey)
            .param("contactSnapshotRevision", recipient.contactSnapshotRevision())
            .update();
    }

    private void updateIntake(UUID intakeId, String state, Long resolutionRevision, UUID notificationId) {
        jdbc.sql("""
                UPDATE notification_intake
                SET state = :state,
                    recipient_resolution_revision = COALESCE(:resolutionRevision,
                        recipient_resolution_revision),
                    notification_id = COALESCE(:notificationId, notification_id),
                    completed_at = CASE WHEN :state IN
                        ('CREATED', 'IGNORED', 'QUARANTINED', 'FAILED_REPAIRABLE')
                        THEN now() ELSE completed_at END,
                    updated_at = now()
                WHERE id = :intakeId
                """)
            .param("state", state)
            .param("resolutionRevision", resolutionRevision)
            .param("notificationId", notificationId)
            .param("intakeId", intakeId)
            .update();
    }

    private IntakeRow findByEventRoute(DomainEventEnvelope envelope, EventRoute route) {
        return jdbc.sql("""
                SELECT id, state, payload_fingerprint, notification_id
                FROM notification_intake
                WHERE consumer_name = :consumerName
                  AND event_id = :eventId
                  AND route_key = :routeKey
                FOR UPDATE
                """)
            .param("consumerName", CONSUMER_NAME)
            .param("eventId", envelope.eventId())
            .param("routeKey", route.routeKey())
            .query(IntakeRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private IntakeRow findByBusinessKey(String businessKey) {
        return jdbc.sql("""
                SELECT id, state, payload_fingerprint, notification_id
                FROM notification_intake
                WHERE business_deduplication_key = :businessKey
                FOR UPDATE
                """)
            .param("businessKey", businessKey)
            .query(IntakeRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private IngestResult replayResult(IntakeRow replay) {
        if ("CREATED".equals(replay.state()) && replay.notificationId() != null) {
            return new IngestResult(Outcome.DUPLICATE, "DUPLICATE_REPLAY", replay.notificationId());
        }
        if ("QUARANTINED".equals(replay.state())) {
            return new IngestResult(Outcome.QUARANTINED, "QUARANTINED", replay.notificationId());
        }
        return new IngestResult(Outcome.IN_PROGRESS, "IN_PROGRESS", replay.notificationId());
    }

    private IngestResult quarantine(
            DomainEventEnvelope envelope,
            UUID intakeId,
            EventRoute route,
            String payloadFingerprint,
            String errorCode,
            String safeMetadata) {
        jdbc.sql("""
                INSERT INTO notification_intake_quarantine(
                    intake_id, event_id, event_type, event_version, aggregate_id,
                    aggregate_version, route_key, route_version, payload_fingerprint,
                    error_code, safe_metadata
                ) VALUES (
                    :intakeId, :eventId, :eventType, :eventVersion, :aggregateId,
                    :aggregateVersion, :routeKey, :routeVersion, :payloadFingerprint,
                    :errorCode, CAST(:safeMetadata AS jsonb)
                )
                """)
            .param("intakeId", intakeId)
            .param("eventId", envelope == null ? null : envelope.eventId())
            .param("eventType", envelope == null ? null : envelope.type())
            .param("eventVersion", envelope == null ? null : envelope.version())
            .param("aggregateId", envelope == null ? null : envelope.aggregateId())
            .param("aggregateVersion", envelope == null ? null : envelope.aggregateVersion())
            .param("routeKey", route == null ? null : route.routeKey())
            .param("routeVersion", route == null ? null : route.routeVersion())
            .param("payloadFingerprint", payloadFingerprint)
            .param("errorCode", errorCode)
            .param("safeMetadata", safeMetadata)
            .update();

        return new IngestResult(Outcome.QUARANTINED, errorCode, null);
    }

    private DomainEventEnvelope parseEnvelope(String rawEnvelope) {
        if (rawEnvelope == null || rawEnvelope.isBlank()) {
            throw new IntakeException("MALFORMED_ENVELOPE", "Envelope is empty");
        }
        final JsonNode root;
        try {
            root = json.readTree(rawEnvelope);
        } catch (Exception invalidJson) {
            throw new IntakeException("MALFORMED_ENVELOPE", "Envelope is not valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw new IntakeException("MALFORMED_ENVELOPE", "Envelope must be an object");
        }
        UUID eventId = uuid(root, "eventId");
        String type = text(root, "type");
        int version = positiveInt(root, "version");
        UUID aggregateId = uuid(root, "aggregateId");
        long aggregateVersion = positiveLong(root, "aggregateVersion");
        UUID correlationId = uuid(root, "correlationId");
        JsonNode causation = root.get("causationId");
        UUID causationId = null;
        if (causation == null) {
            throw new IntakeException("MALFORMED_ENVELOPE", "causationId is required");
        }
        if (!causation.isNull()) {
            causationId = uuid(causation, "causationId");
        }
        String occurredAt = text(root, "occurredAt");
        OffsetDateTime occurred;
        try {
            occurred = OffsetDateTime.parse(occurredAt);
        } catch (Exception invalidDate) {
            throw new IntakeException("MALFORMED_ENVELOPE", "occurredAt is invalid");
        }
        JsonNode data = root.get("data");
        if (data == null || !data.isObject()) {
            throw new IntakeException("SCHEMA_MISMATCH", "data must be an object");
        }
        return new DomainEventEnvelope(
            eventId, type, version, aggregateId, aggregateVersion,
            correlationId, causationId, occurred, data);
    }

    private void validateData(JsonNode data, EventRoute route) {
        for (String field : NotificationEventRouteRegistry.sensitiveFields()) {
            if (data.has(field)) {
                throw new IntakeException("FORBIDDEN_PAYLOAD_FIELD", "Sensitive event field is forbidden");
            }
        }
        if (data.has("recipients")) {
            throw new IntakeException("AMBIGUOUS_RECIPIENT", "Recipient collection requires explicit resolution");
        }
        for (String field : data.propertyNames()) {
            if (!route.safeFields().contains(field)) {
                throw new IntakeException("SCHEMA_MISMATCH", "Event field is not registered for this route");
            }
        }
        if (route.eventType().equals("vistay.auth.account-provisioned")
                && !data.has("accountId") && !data.has("recipientAccountId")) {
            throw new IntakeException("SCHEMA_MISMATCH", "Account provisioned event has no account reference");
        }
    }

    private Projection project(DomainEventEnvelope envelope, EventRoute route) {
        Reference reference = reference(envelope.data());
        String marker = firstText(envelope.data(), STATUS_FIELDS);
        String body = route.defaultBody();
        if (marker != null && marker.matches("[A-Za-z0-9_.-]{1,40}")) {
            body = body + " [" + marker.toUpperCase() + "]";
        }
        String renderFingerprint = fingerprint(
            route.routeKey(), Integer.toString(route.routeVersion()), route.notificationType(),
            route.templateKey(), route.priority(), marker,
            reference == null ? null : reference.type(),
            reference == null ? null : reference.id().toString());
        Map<String, Object> safeData = new LinkedHashMap<>();
        safeData.put("routeKey", route.routeKey());
        safeData.put("routeVersion", route.routeVersion());
        safeData.put("notificationType", route.notificationType());
        safeData.put("priority", route.priority());
        safeData.put("renderInputFingerprint", renderFingerprint);
        if (reference != null) {
            safeData.put("referenceType", reference.type());
            safeData.put("referenceId", reference.id().toString());
        }
        try {
            return new Projection(
                bounded(route.defaultTitle(), 500),
                bounded(body, 2_000),
                renderFingerprint,
                json.writeValueAsString(safeData),
                reference);
        } catch (Exception serializationFailure) {
            throw new IntakeException("SCHEMA_MISMATCH", "Safe notification projection failed");
        }
    }

    private Reference reference(JsonNode data) {
        for (String field : REFERENCE_FIELDS) {
            JsonNode value = data.get(field);
            if (value == null || value.isNull()) {
                continue;
            }
            if (!value.isTextual()) {
                throw new IntakeException("INVALID_REFERENCE", "Reference is not a UUID");
            }
            try {
                return new Reference(referenceType(field), UUID.fromString(value.asText()));
            } catch (IllegalArgumentException invalid) {
                throw new IntakeException("INVALID_REFERENCE", "Reference is not a UUID");
            }
        }
        return null;
    }

    private String notificationCreatedPayload(
            DomainEventEnvelope envelope,
            EventRoute route,
            ResolvedRecipient recipient,
            NotificationRow notification,
            List<String> channels,
            List<String> optionalChannels) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("notificationId", notification.id());
        payload.put("recipientAccountId", recipient.accountId());
        payload.put("recipientAccountType", recipient.accountType().name());
        payload.put("sourceEventId", envelope.eventId());
        payload.put("sourceEventType", envelope.type());
        payload.put("sourceAggregateId", envelope.aggregateId());
        payload.put("sourceAggregateVersion", envelope.aggregateVersion());
        payload.put("routeKey", route.routeKey());
        payload.put("routeVersion", route.routeVersion());
        payload.put("category", route.category());
        payload.put("notificationType", route.notificationType());
        payload.put("priority", route.priority());
        payload.put("templateKey", route.templateKey());
        payload.put("templateRevision", 1);
        payload.put("policyRevision", 1);
        payload.put("channels", channels);
        payload.put("requiredChannels", route.requiredChannels());
        payload.put("optionalChannels", optionalChannels);
        payload.put("inboxRevision", notification.eventSequence());
        payload.put("cursor", Long.toString(notification.eventSequence()));
        payload.put("createdAt", notification.createdAt());
        try {
            return json.writeValueAsString(payload);
        } catch (Exception serializationFailure) {
            throw new IntakeException("SCHEMA_MISMATCH", "Notification event projection failed");
        }
    }

    private static String sourceService(String eventType) {
        String[] parts = eventType.split("\\.");
        return parts.length > 1 ? parts[1] : "unknown";
    }

    private static String referenceType(String field) {
        return switch (field) {
            case "bookingId" -> "BOOKING";
            case "paymentIntentId" -> "PAYMENT_INTENT";
            case "financePaymentRequestId" -> "FINANCE_PAYMENT_REQUEST";
            case "inventoryBlockId" -> "INVENTORY_BLOCK";
            case "publicationId" -> "RATE_PUBLICATION";
            case "propertyId" -> "PROPERTY";
            case "taskId" -> "OPERATIONS_TASK";
            case "caseId" -> "OPERATIONS_CASE";
            case "incidentId" -> "INCIDENT";
            default -> "REFERENCE";
        };
    }

    private static String firstText(JsonNode data, String[] fields) {
        for (String field : fields) {
            JsonNode value = data.get(field);
            if (value != null && value.isTextual() && !value.asText().isBlank()) {
                return value.asText();
            }
        }
        return null;
    }

    private static UUID uuid(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw new IntakeException("MALFORMED_ENVELOPE", field + " is required");
        }
        try {
            return UUID.fromString(value.asText());
        } catch (IllegalArgumentException invalid) {
            throw new IntakeException("MALFORMED_ENVELOPE", field + " is not a UUID");
        }
    }

    private static String text(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw new IntakeException("MALFORMED_ENVELOPE", field + " is required");
        }
        return value.asText();
    }

    private static int positiveInt(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isIntegralNumber() || value.asLong() < 1
                || value.asLong() > Integer.MAX_VALUE) {
            throw new IntakeException("MALFORMED_ENVELOPE", field + " must be a positive integer");
        }
        return value.asInt();
    }

    private static long positiveLong(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isIntegralNumber() || value.asLong() < 1) {
            throw new IntakeException("MALFORMED_ENVELOPE", field + " must be a positive integer");
        }
        return value.asLong();
    }

    private static String bounded(String value, int maximum) {
        return value.length() <= maximum ? value : value.substring(0, maximum);
    }

    private String canonicalPayloadFingerprint(String rawEnvelope) {
        try {
            JsonNode root = json.readTree(rawEnvelope);
            return fingerprint(json.writeValueAsString(canonicalValue(root)));
        } catch (Exception invalid) {
            return fingerprint(rawEnvelope);
        }
    }

    private static Object canonicalValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            Map<String, Object> sorted = new TreeMap<>();
            node.properties().forEach(entry -> sorted.put(entry.getKey(), canonicalValue(entry.getValue())));
            return sorted;
        }
        if (node.isArray()) {
            List<Object> values = new ArrayList<>();
            node.forEach(value -> values.add(canonicalValue(value)));
            return values;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        return node.toString();
    }

    static String fingerprint(String value) {
        return fingerprintValues(value);
    }

    private static String fingerprint(String... values) {
        return fingerprintValues(values);
    }

    private static String fingerprintValues(String... values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                digest.update((value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private String metadataJson(String metadata) {
        return metadata;
    }

    private String metadata(String code, DomainEventEnvelope envelope) {
        return metadata(code, envelope, null);
    }

    private String metadata(String code, DomainEventEnvelope envelope, EventRoute route) {
        Map<String, Object> safe = new LinkedHashMap<>();
        safe.put("code", code);
        if (envelope != null) {
            safe.put("eventId", envelope.eventId());
            safe.put("eventType", envelope.type());
            safe.put("eventVersion", envelope.version());
            safe.put("aggregateId", envelope.aggregateId());
            safe.put("aggregateVersion", envelope.aggregateVersion());
        }
        if (route != null) {
            safe.put("routeKey", route.routeKey());
            safe.put("routeVersion", route.routeVersion());
        }
        try {
            return json.writeValueAsString(safe);
        } catch (Exception serializationFailure) {
            return "{\"code\":\"SCHEMA_MISMATCH\"}";
        }
    }

    public enum Outcome { CREATED, DUPLICATE, IN_PROGRESS, QUARANTINED }

    public record IngestResult(Outcome outcome, String code, UUID notificationId) {}

    public record DomainEventEnvelope(
            UUID eventId,
            String type,
            int version,
            UUID aggregateId,
            long aggregateVersion,
            UUID correlationId,
            UUID causationId,
            OffsetDateTime occurredAt,
            JsonNode data) {}

    private record IntakeRow(UUID id, String state, String payloadFingerprint, UUID notificationId) {}
    private record NotificationRow(UUID id, long eventSequence, OffsetDateTime createdAt) {}
    private record Reference(String type, UUID id) {}
    private record Projection(
            String title,
            String body,
            String renderInputFingerprint,
            String dataJson,
            Reference reference) {}

    private static final class IntakeException extends RuntimeException {
        private final String code;

        private IntakeException(String code, String message) {
            super(message);
            this.code = code;
        }

        private String code() {
            return code;
        }
    }
}
