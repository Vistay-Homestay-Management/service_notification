package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import com.vistay.notification.application.NotificationRecipientResolver.RecipientResolutionException;
import com.vistay.notification.application.NotificationRecipientResolver.ResolvedRecipient;
import com.vistay.notification.application.NotificationContactResolverPort.ContactSnapshot;
import com.vistay.notification.application.NotificationContactResolverPort.VerificationStatus;
import com.vistay.notification.application.NotificationEventRouteRegistry.EventRoute;
import com.vistay.notification.domain.recipient.RecipientDecision;
import com.vistay.notification.domain.recipient.RecipientExceptionType;
import com.vistay.notification.domain.recipient.RecipientKind;
import com.vistay.notification.domain.recipient.ResolutionState;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
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

/**
 * Persists explainable recipient policy evidence without becoming an owner of
 * User identity or contact data. The only contact input accepted here is an
 * immutable, purpose-bound snapshot from the outbound contact port.
 */
@Service
public class RecipientResolutionService {
    private static final String PURPOSE = "NOTIFICATION_DELIVERY";
    private static final String SUMMARY_GROUP = "NOTIFICATION_SUMMARY";
    private static final String DESTINATION_GROUP = "DESTINATION";

    private final JdbcClient jdbc;
    private final ObjectMapper json;
    private final NotificationRecipientResolver resolver;
    private final List<NotificationContactResolverPort> contactPorts;
    private final boolean pushEnabled;

    public RecipientResolutionService(
            JdbcClient jdbc,
            ObjectMapper json,
            NotificationRecipientResolver resolver) {
        this(jdbc, json, resolver, List.of(), false);
    }

    @Autowired
    public RecipientResolutionService(
            JdbcClient jdbc,
            ObjectMapper json,
            NotificationRecipientResolver resolver,
            List<NotificationContactResolverPort> contactPorts,
            @Value("${vistay.notifications.push-enabled:false}") boolean pushEnabled) {
        this.jdbc = jdbc;
        this.json = json;
        this.resolver = resolver;
        this.contactPorts = contactPorts == null ? List.of() : List.copyOf(contactPorts);
        this.pushEnabled = pushEnabled;
    }

    /** Resolves, records decisions, and fails closed for a required contact. */
    @Transactional
    public ResolutionResult resolve(
            DomainEventNotificationService.DomainEventEnvelope envelope,
            JsonNode data,
            EventRoute route) {
        try {
            ResolvedRecipient recipient = resolver.resolve(data, route);
            return recordResolved(envelope, data, route, recipient);
        } catch (RecipientResolutionException failure) {
            throw failure;
        }
    }

    /** Persists a successful resolver result; useful for trusted resolver tests and adapters. */
    @Transactional
    public ResolutionResult recordResolved(
            DomainEventNotificationService.DomainEventEnvelope envelope,
            JsonNode data,
            EventRoute route,
            ResolvedRecipient recipient) {
        RecipientKind kind = recipientKind(data);
        UUID ruleId = ensureRule(route, kind);
        ResolutionRow existing = findResolution(envelope.eventId(), route, recipient, kind);
        if (existing != null) {
            if (existing.state().equals(ResolutionState.INVALID_CONTACT.name())
                    || existing.state().equals(ResolutionState.FAILED_DEPENDENCY.name())) {
                throw new RecipientResolutionException(existing.state(),
                    "Recipient resolution is not eligible for delivery");
            }
            return new ResolutionResult(existing.id(), null, recipient, kind,
                ResolutionState.valueOf(existing.state()), List.of("IN_APP", "EMAIL"), null);
        }

        ContactSnapshot contact;
        try {
            contact = resolveContact(recipient, PURPOSE, DESTINATION_GROUP);
        } catch (ContactResolutionFailure dependencyFailure) {
            UUID resolutionId = insertResolution(envelope, route, ruleId, kind, recipient,
                ResolutionState.FAILED_DEPENDENCY, null);
            UUID candidateId = insertCandidate(resolutionId, recipient, kind, "USER_CONTACT_API",
                "REJECTED", dependencyFailure.code());
            insertDecision(resolutionId, candidateId, "EMAIL", RecipientDecision.DENY,
                dependencyFailure.code(), DESTINATION_GROUP, null);
            recordException(resolutionId, null, authorized(recipient), kind,
                RecipientExceptionType.DEPENDENCY_UNAVAILABLE, dependencyFailure.code(), null);
            throw new RecipientResolutionException("DEPENDENCY_UNAVAILABLE",
                "Recipient contact dependency is unavailable");
        }

        UUID contactId = persistContactSnapshot(recipient, contact);
        UUID resolutionId = insertResolution(envelope, route, ruleId, kind, recipient,
            ResolutionState.PENDING, contact == null ? null : contact.revision());
        UUID candidateId = insertCandidate(resolutionId, recipient, kind, "USER_AUTHORIZATION",
            "SELECTED", "AUTHORIZED_RECIPIENT");

        List<String> allowed = new ArrayList<>();
        for (String channel : route.requiredChannels()) {
            if ("EMAIL".equals(channel) && !verified(contact)) {
                String code = contact == null ? "MISSING_CONTACT" : contactCode(contact);
                insertDecision(resolutionId, candidateId, channel, RecipientDecision.DENY,
                    code, DESTINATION_GROUP, contact);
                updateResolutionState(resolutionId, ResolutionState.INVALID_CONTACT, contact);
                recordException(resolutionId, null, authorized(recipient), kind,
                    exceptionType(code), code, contact);
                throw new RecipientResolutionException(code,
                    "Required recipient contact is not eligible");
            }
            insertDecision(resolutionId, candidateId, channel, RecipientDecision.ALLOW,
                "REQUIRED_CHANNEL", "IN_APP".equals(channel) ? SUMMARY_GROUP : DESTINATION_GROUP,
                contact);
            allowed.add(channel);
        }

        ResolutionState state = ResolutionState.RESOLVED;
        for (String channel : route.optionalChannels()) {
            if (!"PUSH".equals(channel)) {
                continue;
            }
            if (pushEnabled && validDevice(recipient.deviceReference())) {
                insertDecision(resolutionId, candidateId, channel, RecipientDecision.ALLOW,
                    "OPTIONAL_ELIGIBLE", DESTINATION_GROUP, contact);
                allowed.add(channel);
            } else {
                state = ResolutionState.PARTIALLY_RESOLVED;
                insertDecision(resolutionId, candidateId, channel, RecipientDecision.DENY,
                    "OPTIONAL_SUPPRESSED", DESTINATION_GROUP, contact);
                recordException(resolutionId, null, authorized(recipient), kind,
                    RecipientExceptionType.OPTIONAL_SUPPRESSED, "OPTIONAL_SUPPRESSED", contact);
            }
        }
        updateResolutionState(resolutionId, state, contact);
        return new ResolutionResult(resolutionId, contactId, recipient, kind, state,
            List.copyOf(allowed), contact);
    }

    /** Records a failed resolution even when no notification row can be created. */
    @Transactional
    public void recordFailure(
            DomainEventNotificationService.DomainEventEnvelope envelope,
            JsonNode data,
            EventRoute route,
            String code) {
        RecipientKind kind = recipientKind(data);
        UUID ruleId = ensureRule(route, kind);
        UUID accountId = accountId(data);
        String accountType = accountType(data);
        if (accountId == null || accountType == null
                || !("CUSTOMER".equals(accountType) || "ADMIN".equals(accountType)
                    || "HOST".equals(accountType))) {
            accountType = null;
        }
        ResolutionRow existing = findFailureResolution(envelope.eventId(), route, code);
        if (existing != null) {
            return;
        }
        UUID resolutionId = jdbc.sql("""
            INSERT INTO notification_recipient_resolutions(
                event_id, event_type, route_key, route_version, rule_id,
                resolution_revision, recipient_account_type, recipient_user_id,
                recipient_kind, state, purpose, field_group, safe_metadata,
                correlation_id
            ) VALUES (:eventId, :eventType, :routeKey, :routeVersion, :ruleId,
                1, :accountType, :accountId, :recipientKind, :state, :purpose,
                :fieldGroup, CAST(:metadata AS jsonb), :correlationId)
            RETURNING id
            """)
            .param("eventId", envelope.eventId())
            .param("eventType", envelope.type())
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("ruleId", ruleId)
            .param("accountType", accountType)
            .param("accountId", accountId)
            .param("recipientKind", kind.name())
            .param("state", failureState(code).name())
            .param("purpose", PURPOSE)
            .param("fieldGroup", SUMMARY_GROUP)
            .param("metadata", metadata(route, code, kind))
            .param("correlationId", envelope.correlationId().toString())
            .query(UUID.class)
            .single();
        NotificationAuthorizationService.AuthorizedRecipient recipient = accountId == null
            ? null : new NotificationAuthorizationService.AuthorizedRecipient(accountType, accountId);
        recordException(resolutionId, null, recipient, kind, exceptionType(code), code, null);
        writeResolutionEvent(resolutionId, null, code, envelope.correlationId().toString());
    }

    /** Links the evidence to the created notification and its immutable delivery rows. */
    @Transactional
    public void linkNotification(ResolutionResult result, UUID notificationId) {
        jdbc.sql("""
            UPDATE notification_recipient_resolutions
            SET notification_id = :notificationId, updated_at = now()
            WHERE id = :resolutionId
            """)
            .param("notificationId", notificationId)
            .param("resolutionId", result.resolutionId())
            .update();
        jdbc.sql("""
            UPDATE notifications
            SET recipient_resolution_id = :resolutionId, updated_at = now()
            WHERE id = :notificationId
            """)
            .param("resolutionId", result.resolutionId())
            .param("notificationId", notificationId)
            .update();
        for (String channel : result.allowedChannels()) {
            jdbc.sql("""
                UPDATE notification_deliveries
                SET recipient_resolution_id = :resolutionId,
                    contact_snapshot_id = COALESCE(:contactSnapshotId, contact_snapshot_id),
                    destination_masked = COALESCE(:destinationMasked, destination_masked),
                    destination_ciphertext = COALESCE(:destinationCiphertext, destination_ciphertext),
                    destination_resolution_state = CASE
                        WHEN channel = 'IN_APP' OR :hasVerifiedContact THEN 'RESOLVED'
                        ELSE destination_resolution_state END,
                    updated_at = now()
                WHERE notification_id = :notificationId AND channel = :channel
                """)
                .param("resolutionId", result.resolutionId())
                .param("contactSnapshotId", result.contactSnapshotId())
                .param("destinationMasked", result.contact() == null
                    ? null : result.contact().destinationMasked())
                .param("destinationCiphertext", result.contact() == null
                    ? null : result.contact().destinationCiphertext())
                .param("hasVerifiedContact", verified(result.contact()))
                .param("notificationId", notificationId)
                .param("channel", channel)
                .update();
        }
        writeResolutionEvent(result.resolutionId(), notificationId,
            result.state().name(), null);
    }

    /** Creates a linked child resolution after an operator requests re-resolution. */
    @Transactional
    public ReResolveResult reResolve(UUID exceptionId) {
        ExceptionRow exception = jdbc.sql("""
            SELECT e.id, e.resolution_id, e.notification_id, e.recipient_account_id,
                   r.recipient_account_type, r.recipient_kind, r.route_key,
                   r.route_version, r.rule_id, r.resolution_revision,
                   r.contact_snapshot_revision
            FROM notification_recipient_exceptions e
            JOIN notification_recipient_resolutions r ON r.id = e.resolution_id
            WHERE e.id = :exceptionId
            FOR UPDATE
            """)
            .param("exceptionId", exceptionId)
            .query(ExceptionRow.class)
            .list()
            .stream().findFirst()
            .orElseThrow(() -> new NotificationCommandException("NOT_FOUND",
                "Recipient exception was not found"));
        if (exception.recipientAccountId() == null || exception.recipientAccountType() == null) {
            throw new NotificationCommandException("DEPENDENCY_UNAVAILABLE",
                "Recipient cannot be re-resolved without an account reference");
        }
        ContactSnapshot contact;
        try {
            contact = resolveLatestContact(exception.recipientAccountType(),
                exception.recipientAccountId());
        } catch (ContactResolutionFailure dependencyFailure) {
            throw new NotificationCommandException("DEPENDENCY_UNAVAILABLE",
                "Recipient contact dependency is unavailable");
        }
        long nextRevision = exception.resolutionRevision() + 1;
        UUID contactSnapshotId = persistContactSnapshot(exception.recipientAccountType(),
            exception.recipientAccountId(), contact);
        UUID childId = jdbc.sql("""
            INSERT INTO notification_recipient_resolutions(
                notification_id, route_key, route_version, rule_id,
                resolution_revision, recipient_account_type, recipient_user_id,
                recipient_kind, contact_snapshot_revision, state, parent_resolution_id,
                purpose, field_group, safe_metadata
            ) VALUES (:notificationId, :routeKey, :routeVersion, :ruleId,
                :revision, :accountType, :accountId, :recipientKind, :contactRevision,
                :state, :parentId, :purpose, :fieldGroup, CAST(:metadata AS jsonb))
            RETURNING id
            """)
            .param("notificationId", exception.notificationId())
            .param("routeKey", exception.routeKey())
            .param("routeVersion", exception.routeVersion())
            .param("ruleId", exception.ruleId())
            .param("revision", nextRevision)
            .param("accountType", exception.recipientAccountType())
            .param("accountId", exception.recipientAccountId())
            .param("recipientKind", exception.recipientKind())
            .param("contactRevision", contact == null ? null : contact.revision())
            .param("state", verified(contact) ? ResolutionState.RESOLVED.name()
                : ResolutionState.INVALID_CONTACT.name())
            .param("parentId", exception.resolutionId())
            .param("purpose", PURPOSE)
            .param("fieldGroup", SUMMARY_GROUP)
            .param("metadata", metadata(exception.routeKey(), exception.recipientKind(),
                "RE_RESOLVE"))
            .query(UUID.class)
            .single();
        UUID candidateId = jdbc.sql("""
            INSERT INTO notification_recipient_candidates(
                resolution_id, candidate_key, recipient_account_type, recipient_user_id,
                recipient_kind, resolution_source, state, reason_code
            ) VALUES (:resolutionId, :candidateKey, :accountType, :accountId,
                :recipientKind, 'USER_CONTACT_API', :state, :reasonCode)
            RETURNING id
            """)
            .param("resolutionId", childId)
            .param("candidateKey", exception.recipientAccountId().toString())
            .param("accountType", exception.recipientAccountType())
            .param("accountId", exception.recipientAccountId())
            .param("recipientKind", exception.recipientKind())
            .param("state", verified(contact) ? "SELECTED" : "REJECTED")
            .param("reasonCode", verified(contact) ? "CONTACT_RE_RESOLVED" : "MISSING_CONTACT")
            .query(UUID.class)
            .single();
        insertDecision(childId, candidateId, "IN_APP", RecipientDecision.ALLOW,
            "REQUIRED_CHANNEL", SUMMARY_GROUP, contact);
        insertDecision(childId, candidateId, "EMAIL",
            verified(contact) ? RecipientDecision.ALLOW : RecipientDecision.DENY,
            verified(contact) ? "REQUIRED_CHANNEL" : "MISSING_CONTACT", DESTINATION_GROUP, contact);
        if (verified(contact) && exception.notificationId() != null) {
            refreshDeliveries(childId, exception.notificationId(), contactSnapshotId, contact);
            writeResolutionEvent(childId, exception.notificationId(), ResolutionState.RESOLVED.name(), null);
        }
        return new ReResolveResult(childId, exception.notificationId(), contactSnapshotId, contact,
            verified(contact) ? ResolutionState.RESOLVED : ResolutionState.INVALID_CONTACT);
    }

    static RecipientKind recipientKind(JsonNode data) {
        if (has(data, "subscriptionId") || has(data, "recipientSubscriptionId")) {
            return RecipientKind.EXPLICIT_SUBSCRIPTION;
        }
        if (has(data, "bookingId") || has(data, "guestAccountId") || has(data, "hostAccountId")) {
            return RecipientKind.BOOKING_PARTY;
        }
        if (has(data, "branchId") || has(data, "siteId")) {
            return RecipientKind.BRANCH_ROLE;
        }
        if (has(data, "organizationId") || has(data, "role")) {
            return RecipientKind.ORGANIZATION_ROLE;
        }
        return RecipientKind.DIRECT_ACCOUNT;
    }

    static String contactCode(ContactSnapshot contact) {
        return validateContactSnapshot(contact);
    }

    static String validateContactSnapshot(ContactSnapshot contact) {
        if (contact == null || contact.revision() < 1) {
            return "MISSING_CONTACT";
        }
        if (contact.snapshotFingerprint() != null
                && !contact.snapshotFingerprint().matches("[0-9a-f]{64}")) {
            return "INVALID_CONTACT";
        }
        if (contact.verificationStatus() == VerificationStatus.UNVERIFIED) {
            return "UNVERIFIED_CONTACT";
        }
        if (contact.verificationStatus() == VerificationStatus.INVALID) {
            return "INVALID_CONTACT";
        }
        if (contact.verificationStatus() == VerificationStatus.MISSING) {
            return "MISSING_CONTACT";
        }
        if (contact.destinationMasked() == null || contact.destinationMasked().isBlank()
                || contact.destinationCiphertext() == null
                || contact.destinationCiphertext().length == 0) {
            return "MISSING_CONTACT";
        }
        return "CONTACT_VERIFIED";
    }

    private UUID ensureRule(EventRoute route, RecipientKind kind) {
        jdbc.sql("""
            INSERT INTO notification_recipient_rules(
                rule_key, route_key, route_version, recipient_kind, revision,
                required_channels, optional_channels, source_fields
            ) VALUES (:ruleKey, :routeKey, :routeVersion, :recipientKind, 1,
                CAST(:requiredChannels AS jsonb), CAST(:optionalChannels AS jsonb),
                CAST(:sourceFields AS jsonb))
            ON CONFLICT (route_key, route_version, recipient_kind) DO NOTHING
            """)
            .param("ruleKey", route.routeKey() + ":" + kind.name())
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("recipientKind", kind.name())
            .param("requiredChannels", json(route.requiredChannels()))
            .param("optionalChannels", json(route.optionalChannels()))
            .param("sourceFields", json(Map.of("resolver", "UserAuthorization", "version", 1)))
            .update();
        return jdbc.sql("""
            SELECT id
            FROM notification_recipient_rules
            WHERE route_key = :routeKey AND route_version = :routeVersion
              AND recipient_kind = :recipientKind AND active = true
            """)
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("recipientKind", kind.name())
            .query(UUID.class)
            .single();
    }

    private UUID insertResolution(
            DomainEventNotificationService.DomainEventEnvelope envelope,
            EventRoute route,
            UUID ruleId,
            RecipientKind kind,
            ResolvedRecipient recipient,
            ResolutionState state,
            Long contactRevision) {
        return jdbc.sql("""
            INSERT INTO notification_recipient_resolutions(
                event_id, event_type, route_key, route_version, rule_id,
                resolution_revision, recipient_account_type, recipient_user_id,
                recipient_kind, contact_snapshot_revision, state, purpose,
                field_group, safe_metadata, correlation_id
            ) VALUES (:eventId, :eventType, :routeKey, :routeVersion, :ruleId,
                :revision, :accountType, :accountId, :recipientKind, :contactRevision,
                :state, :purpose, :fieldGroup, CAST(:metadata AS jsonb), :correlationId)
            RETURNING id
            """)
            .param("eventId", envelope.eventId())
            .param("eventType", envelope.type())
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("ruleId", ruleId)
            .param("revision", recipient.resolutionRevision())
            .param("accountType", recipient.accountType().name())
            .param("accountId", recipient.accountId())
            .param("recipientKind", kind.name())
            .param("contactRevision", contactRevision)
            .param("state", state.name())
            .param("purpose", PURPOSE)
            .param("fieldGroup", SUMMARY_GROUP)
            .param("metadata", metadata(route, "RESOLUTION_PENDING", kind))
            .param("correlationId", envelope.correlationId().toString())
            .query(UUID.class)
            .single();
    }

    private UUID insertCandidate(
            UUID resolutionId,
            ResolvedRecipient recipient,
            RecipientKind kind,
            String source,
            String state,
            String reasonCode) {
        return jdbc.sql("""
            INSERT INTO notification_recipient_candidates(
                resolution_id, candidate_key, recipient_account_type, recipient_user_id,
                recipient_kind, resolution_source, state, reason_code
            ) VALUES (:resolutionId, :candidateKey, :accountType, :accountId,
                :recipientKind, :source, :state, :reasonCode)
            RETURNING id
            """)
            .param("resolutionId", resolutionId)
            .param("candidateKey", recipient.accountType().name() + ":" + recipient.accountId())
            .param("accountType", recipient.accountType().name())
            .param("accountId", recipient.accountId())
            .param("recipientKind", kind.name())
            .param("source", source)
            .param("state", state)
            .param("reasonCode", reasonCode)
            .query(UUID.class)
            .single();
    }

    private void insertDecision(
            UUID resolutionId,
            UUID candidateId,
            String channel,
            RecipientDecision decision,
            String reasonCode,
            String fieldGroup,
            ContactSnapshot contact) {
        jdbc.sql("""
            INSERT INTO notification_recipient_decisions(
                resolution_id, candidate_id, channel, decision, reason_code,
                purpose, field_group, destination_masked, contact_snapshot_revision
            ) VALUES (:resolutionId, :candidateId, :channel, :decision, :reasonCode,
                :purpose, :fieldGroup, :destinationMasked, :contactRevision)
            ON CONFLICT (resolution_id, candidate_id, channel) DO NOTHING
            """)
            .param("resolutionId", resolutionId)
            .param("candidateId", candidateId)
            .param("channel", channel)
            .param("decision", decision.name())
            .param("reasonCode", reasonCode)
            .param("purpose", PURPOSE)
            .param("fieldGroup", fieldGroup)
            .param("destinationMasked", contact == null ? null : contact.destinationMasked())
            .param("contactRevision", contact == null ? null : contact.revision())
            .update();
    }

    private void updateResolutionState(UUID resolutionId, ResolutionState state, ContactSnapshot contact) {
        jdbc.sql("""
            UPDATE notification_recipient_resolutions
            SET state = :state, contact_snapshot_revision = COALESCE(:contactRevision,
                contact_snapshot_revision), updated_at = now()
            WHERE id = :resolutionId
            """)
            .param("state", state.name())
            .param("contactRevision", contact == null ? null : contact.revision())
            .param("resolutionId", resolutionId)
            .update();
    }

    private void refreshDeliveries(
            UUID resolutionId,
            UUID notificationId,
            UUID contactSnapshotId,
            ContactSnapshot contact) {
        jdbc.sql("""
            UPDATE notification_deliveries
            SET recipient_resolution_id = :resolutionId,
                contact_snapshot_id = :contactSnapshotId,
                destination_masked = :destinationMasked,
                destination_ciphertext = :destinationCiphertext,
                destination_resolution_state = 'RESOLVED',
                status = CASE WHEN status IN ('FAILED', 'DEAD_LETTERED', 'PENDING')
                              THEN 'PENDING' ELSE status END,
                attempt_count = CASE WHEN status IN ('FAILED', 'DEAD_LETTERED', 'PENDING')
                                     THEN 0 ELSE attempt_count END,
                next_attempt_at = CASE WHEN status IN ('FAILED', 'DEAD_LETTERED', 'PENDING')
                                       THEN now() ELSE next_attempt_at END,
                last_error_code = NULL, error_class = NULL,
                delivery_revision = delivery_revision + 1, updated_at = now()
            WHERE notification_id = :notificationId
              AND channel <> 'IN_APP'
              AND status IN ('FAILED', 'DEAD_LETTERED', 'PENDING')
            """)
            .param("resolutionId", resolutionId)
            .param("contactSnapshotId", contactSnapshotId)
            .param("destinationMasked", contact.destinationMasked())
            .param("destinationCiphertext", contact.destinationCiphertext())
            .param("notificationId", notificationId)
            .update();
    }

    private UUID persistContactSnapshot(ResolvedRecipient recipient, ContactSnapshot contact) {
        return persistContactSnapshot(recipient.accountType().name(), recipient.accountId(), contact);
    }

    private UUID persistContactSnapshot(
            String accountType,
            UUID accountId,
            ContactSnapshot contact) {
        if (contact == null || contact.revision() < 1) {
            return null;
        }
        String fingerprint = contact.snapshotFingerprint() == null
                || !contact.snapshotFingerprint().matches("[0-9a-f]{64}")
            ? fingerprint(accountType, accountId.toString(),
                Long.toString(contact.revision()), String.valueOf(contact.verificationStatus()))
            : contact.snapshotFingerprint();
        jdbc.sql("""
            INSERT INTO notification_contact_snapshots(
                recipient_account_type, recipient_user_id, revision, purpose,
                field_group, destination_masked, destination_ciphertext,
                verification_status, snapshot_fingerprint, source_reference
            ) VALUES (:accountType, :accountId, :revision, :purpose, :fieldGroup,
                :destinationMasked, :destinationCiphertext, :verificationStatus,
                :fingerprint, :sourceReference)
            ON CONFLICT (recipient_account_type, recipient_user_id, revision, purpose)
            DO NOTHING
            """)
            .param("accountType", accountType)
            .param("accountId", accountId)
            .param("revision", contact.revision())
            .param("purpose", PURPOSE)
            .param("fieldGroup", DESTINATION_GROUP)
            .param("destinationMasked", contact.destinationMasked())
            .param("destinationCiphertext", contact.destinationCiphertext())
            .param("verificationStatus", contact.verificationStatus().name())
            .param("fingerprint", fingerprint)
            .param("sourceReference", contact.sourceReference())
            .update();
        return jdbc.sql("""
            SELECT id
            FROM notification_contact_snapshots
            WHERE recipient_account_type = :accountType AND recipient_user_id = :accountId
              AND revision = :revision AND purpose = :purpose
            """)
            .param("accountType", accountType)
            .param("accountId", accountId)
            .param("revision", contact.revision())
            .param("purpose", PURPOSE)
            .query(UUID.class)
            .single();
    }

    private void recordException(
            UUID resolutionId,
            UUID notificationId,
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            RecipientKind kind,
            RecipientExceptionType type,
            String reasonCode,
            ContactSnapshot contact) {
        UUID exceptionId = jdbc.sql("""
            INSERT INTO notification_recipient_exceptions(
                resolution_id, notification_id, recipient_account_id, recipient_kind,
                exception_type, status, reason_code, destination_masked,
                contact_snapshot_revision, correlation_id
            ) VALUES (:resolutionId, :notificationId, :accountId, :recipientKind,
                :exceptionType, 'OPEN', :reasonCode, :destinationMasked,
                :contactRevision, :correlationId)
            ON CONFLICT (resolution_id, exception_type)
            WHERE status IN ('OPEN', 'IN_REVIEW') DO UPDATE
            SET updated_at = now()
            RETURNING id
            """)
            .param("resolutionId", resolutionId)
            .param("notificationId", notificationId)
            .param("accountId", recipient == null ? null : recipient.accountId())
            .param("recipientKind", kind.name())
            .param("exceptionType", type.name())
            .param("reasonCode", reasonCode)
            .param("destinationMasked", contact == null ? null : contact.destinationMasked())
            .param("contactRevision", contact == null ? null : contact.revision())
            .param("correlationId", resolutionId.toString())
            .query(UUID.class)
            .single();
        writeResolutionHistory(exceptionId, resolutionId, "RECORD_FAILURE", "OPEN", reasonCode);
    }

    private void writeResolutionHistory(
            UUID exceptionId,
            UUID resolutionId,
            String action,
            String toStatus,
            String reason) {
        jdbc.sql("""
            INSERT INTO notification_recipient_resolution_history(
                exception_id, resolution_id, to_status, action, reason,
                safe_metadata
            ) VALUES (:exceptionId, :resolutionId, :toStatus, :action, :reason,
                CAST(:metadata AS jsonb))
            ON CONFLICT (action, idempotency_key) DO NOTHING
            """)
            .param("exceptionId", exceptionId)
            .param("resolutionId", resolutionId)
            .param("toStatus", toStatus)
            .param("action", action)
            .param("reason", reason)
            .param("metadata", json(Map.of("fieldGroup", SUMMARY_GROUP)))
            .update();
    }

    private void writeResolutionEvent(
            UUID resolutionId,
            UUID notificationId,
            String state,
            String correlationId) {
        UUID aggregateId = notificationId == null ? resolutionId : notificationId;
        jdbc.sql("""
            INSERT INTO audit_events(action, aggregate_type, aggregate_id, changes, correlation_id)
            VALUES ('NOTIFICATION_RECIPIENT_RESOLUTION', 'NotificationRecipientResolution',
                :aggregateId, CAST(:changes AS jsonb), :correlationId)
            """)
            .param("aggregateId", aggregateId)
            .param("changes", json(Map.of("resolutionId", resolutionId.toString(), "state", state)))
            .param("correlationId", correlationId)
            .update();
        jdbc.sql("""
            INSERT INTO outbox_events(
                aggregate_type, aggregate_id, aggregate_version, event_type,
                event_version, payload, correlation_id, causation_id
            ) VALUES ('NotificationRecipientResolution', :aggregateId, 1,
                'vistay.notification.recipient-exception-recorded', 1,
                CAST(:payload AS jsonb), :correlationId, :causationId)
            """)
            .param("aggregateId", aggregateId)
            .param("payload", json(Map.of(
                "resolutionId", resolutionId.toString(), "state", state,
                "notificationId", notificationId == null ? "" : notificationId.toString())))
            .param("correlationId", correlationId)
            .param("causationId", resolutionId.toString())
            .update();
    }

    private ContactSnapshot resolveContact(
            ResolvedRecipient recipient,
            String purpose,
            String fieldGroup) {
        for (NotificationContactResolverPort port : contactPorts) {
            try {
                ContactSnapshot result = port.resolve(
                    recipient.accountType(), recipient.accountId(), purpose, fieldGroup);
                if (result != null) {
                    return result;
                }
            } catch (RuntimeException dependencyFailure) {
                throw new ContactResolutionFailure("DEPENDENCY_UNAVAILABLE", dependencyFailure);
            }
        }
        throw new ContactResolutionFailure("DEPENDENCY_UNAVAILABLE",
            new IllegalStateException("No User contact snapshot port is configured"));
    }

    private ContactSnapshot resolveLatestContact(String accountType, UUID accountId) {
        AccountKind kind;
        try {
            kind = AccountKind.valueOf(accountType);
        } catch (IllegalArgumentException invalidAccountType) {
            throw new ContactResolutionFailure("DEPENDENCY_UNAVAILABLE", invalidAccountType);
        }
        for (NotificationContactResolverPort port : contactPorts) {
            try {
                ContactSnapshot result = port.resolve(kind, accountId, PURPOSE, DESTINATION_GROUP);
                if (result != null) {
                    return result;
                }
            } catch (RuntimeException dependencyFailure) {
                throw new ContactResolutionFailure("DEPENDENCY_UNAVAILABLE", dependencyFailure);
            }
        }
        throw new ContactResolutionFailure("DEPENDENCY_UNAVAILABLE",
            new IllegalStateException("No User contact snapshot port is configured"));
    }

    private ResolutionRow findResolution(
            UUID eventId,
            EventRoute route,
            ResolvedRecipient recipient,
            RecipientKind kind) {
        return jdbc.sql("""
            SELECT id, state, contact_snapshot_revision
            FROM notification_recipient_resolutions
            WHERE event_id = :eventId AND route_key = :routeKey
              AND route_version = :routeVersion
              AND recipient_account_type = :accountType
              AND recipient_user_id = :accountId
              AND recipient_kind = :recipientKind
              AND resolution_revision = :revision
            """)
            .param("eventId", eventId)
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("accountType", recipient.accountType().name())
            .param("accountId", recipient.accountId())
            .param("recipientKind", kind.name())
            .param("revision", recipient.resolutionRevision())
            .query(ResolutionRow.class)
            .list().stream().findFirst().orElse(null);
    }

    private ResolutionRow findFailureResolution(UUID eventId, EventRoute route, String code) {
        return jdbc.sql("""
            SELECT r.id, r.state, r.contact_snapshot_revision
            FROM notification_recipient_resolutions r
            JOIN notification_recipient_exceptions e ON e.resolution_id = r.id
            WHERE r.event_id = :eventId AND r.route_key = :routeKey
              AND r.route_version = :routeVersion AND e.reason_code = :reasonCode
            """)
            .param("eventId", eventId)
            .param("routeKey", route.routeKey())
            .param("routeVersion", route.routeVersion())
            .param("reasonCode", code)
            .query(ResolutionRow.class)
            .list().stream().findFirst().orElse(null);
    }

    private static RecipientExceptionType exceptionType(String code) {
        try {
            return RecipientExceptionType.valueOf(code);
        } catch (IllegalArgumentException ignored) {
            return code.startsWith("AUTHORIZATION")
                ? RecipientExceptionType.DEPENDENCY_UNAVAILABLE
                : RecipientExceptionType.INVALID_CONTACT;
        }
    }

    private static ResolutionState failureState(String code) {
        if ("NO_RECIPIENT".equals(code)) {
            return ResolutionState.NO_RECIPIENT;
        }
        if ("DEPENDENCY_UNAVAILABLE".equals(code) || code.startsWith("AUTHORIZATION")) {
            return ResolutionState.FAILED_DEPENDENCY;
        }
        return ResolutionState.INVALID_CONTACT;
    }

    private static boolean verified(ContactSnapshot contact) {
        return "CONTACT_VERIFIED".equals(validateContactSnapshot(contact));
    }

    private static boolean validDevice(String reference) {
        return reference != null && reference.matches("[A-Za-z0-9._:-]{1,180}");
    }

    private static NotificationAuthorizationService.AuthorizedRecipient authorized(
            ResolvedRecipient recipient) {
        return new NotificationAuthorizationService.AuthorizedRecipient(
            recipient.accountType().name(), recipient.accountId());
    }

    private static boolean has(JsonNode data, String field) {
        return data != null && data.has(field) && !data.get(field).isNull();
    }

    private static UUID accountId(JsonNode data) {
        String value = firstText(data, "recipientAccountId", "recipientUserId", "accountId");
        try {
            return value == null ? null : UUID.fromString(value);
        } catch (IllegalArgumentException invalid) {
            return null;
        }
    }

    private static String accountType(JsonNode data) {
        String value = firstText(data, "recipientAccountType", "accountType", "realm");
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toUpperCase();
        return "BACKOFFICE".equals(normalized) ? "ADMIN" : normalized;
    }

    private static String firstText(JsonNode data, String... fields) {
        if (data == null) {
            return null;
        }
        for (String field : fields) {
            JsonNode value = data.get(field);
            if (value != null && value.isTextual() && !value.asText().isBlank()) {
                return value.asText();
            }
        }
        return null;
    }

    private String metadata(EventRoute route, String code, RecipientKind kind) {
        return json(Map.of("routeKey", route.routeKey(), "routeVersion", route.routeVersion(),
            "reasonCode", code, "recipientKind", kind.name(), "fieldGroup", SUMMARY_GROUP));
    }

    private String metadata(String routeKey, String kind, String action) {
        return json(Map.of("routeKey", routeKey, "recipientKind", kind, "action", action,
            "fieldGroup", SUMMARY_GROUP));
    }

    private String json(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception failure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH",
                "Recipient resolution evidence cannot be serialized");
        }
    }

    static String fingerprint(String... values) {
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

    public record ResolutionResult(
            UUID resolutionId,
            UUID contactSnapshotId,
            ResolvedRecipient recipient,
            RecipientKind recipientKind,
            ResolutionState state,
            List<String> allowedChannels,
            ContactSnapshot contact) {
        public ResolutionResult {
            allowedChannels = List.copyOf(allowedChannels == null ? List.of() : allowedChannels);
        }
    }

    public record ReResolveResult(
            UUID resolutionId,
            UUID notificationId,
            UUID contactSnapshotId,
            ContactSnapshot contact,
            ResolutionState state) {}

    private record ResolutionRow(UUID id, String state, Long contactSnapshotRevision) {}

    private record ExceptionRow(
            UUID id,
            UUID resolutionId,
            UUID notificationId,
            UUID recipientAccountId,
            String recipientAccountType,
            String recipientKind,
            String routeKey,
            int routeVersion,
            UUID ruleId,
            long resolutionRevision,
            Long contactSnapshotRevision) {}

    private static final class ContactResolutionFailure extends RuntimeException {
        private final String code;

        private ContactResolutionFailure(String code, Throwable cause) {
            super(cause);
            this.code = code;
        }

        private String code() {
            return code;
        }
    }
}
