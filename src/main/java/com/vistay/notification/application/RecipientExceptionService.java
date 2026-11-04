package com.vistay.notification.application;

import com.vistay.notification.domain.recipient.RecipientExceptionStatus;
import com.vistay.notification.domain.recipient.RecipientExceptionType;
import com.vistay.notification.domain.recipient.ResolutionAction;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** Auditable operator repair boundary for recipient-resolution exceptions. */
@Service
public class RecipientExceptionService {
    private static final String RESOLVE_OPERATION = "notification.recipient-exception.resolve";
    private static final int MAX_PAGE_SIZE = 100;

    private final JdbcClient jdbc;
    private final ObjectMapper json;
    private final RecipientResolutionService resolutions;

    public RecipientExceptionService(
            JdbcClient jdbc,
            ObjectMapper json,
            RecipientResolutionService resolutions) {
        this.jdbc = jdbc;
        this.json = json;
        this.resolutions = resolutions;
    }

    @Transactional
    public ExceptionConnection list(ExceptionQuery query) {
        int limit = boundedLimit(query.limit());
        long afterCursor = query.afterCursor() == null
            ? 0 : NotificationInboxService.parseCursor(query.afterCursor());
        String status = enumValue(query.status(), RecipientExceptionStatus.class, "status");
        String exceptionType = enumValue(query.exceptionType(), RecipientExceptionType.class,
            "exceptionType");
        StringBuilder sql = new StringBuilder("""
            SELECT e.exception_sequence, e.id, e.notification_id, e.recipient_account_id,
                   e.recipient_kind, e.exception_type, e.status, e.reason_code,
                   e.resolution_action, e.destination_masked, e.contact_snapshot_revision,
                   e.revision, e.created_at, e.resolved_at, e.resolution_id
            FROM notification_recipient_exceptions e
            WHERE e.exception_sequence > :afterCursor
            """);
        if (status != null) {
            sql.append(" AND e.status = :status\n");
        }
        if (exceptionType != null) {
            sql.append(" AND e.exception_type = :exceptionType\n");
        }
        sql.append(" ORDER BY e.exception_sequence LIMIT :limitPlusOne");
        var statement = jdbc.sql(sql.toString())
            .param("afterCursor", afterCursor)
            .param("limitPlusOne", limit + 1);
        if (status != null) {
            statement = statement.param("status", status);
        }
        if (exceptionType != null) {
            statement = statement.param("exceptionType", exceptionType);
        }
        List<ExceptionRow> rows = statement.query(ExceptionRow.class).list();
        boolean hasNext = rows.size() > limit;
        if (hasNext) {
            rows = new ArrayList<>(rows.subList(0, limit));
        }
        Long start = rows.isEmpty() ? null : rows.get(0).exceptionSequence();
        Long end = rows.isEmpty() ? null : rows.get(rows.size() - 1).exceptionSequence();
        return new ExceptionConnection(rows.stream().map(this::map).toList(),
            new PageInfo(hasNext, end, start, "ACTIVE", 0, null));
    }

    @Transactional
    public ExceptionView resolve(ResolveCommand command) {
        validate(command);
        String fingerprint = DomainEventNotificationService.fingerprint(
            RESOLVE_OPERATION, command.exceptionId().toString(), command.action().name(),
            Integer.toString(command.expectedVersion()), command.reason(),
            command.actorAccountType(), command.actorAccountId().toString());
        IdempotencyRow previous = findIdempotency(command.idempotencyKey());
        if (previous != null) {
            if (!fingerprint.equals(previous.requestFingerprint())) {
                throw new NotificationCommandException("IDEMPOTENCY_KEY_REUSED",
                    "Idempotency key was already used for another request");
            }
            return get(command.exceptionId());
        }

        ExceptionRow before = loadForUpdate(command.exceptionId());
        if (before == null) {
            throw new NotificationCommandException("NOT_FOUND",
                "Recipient exception was not found");
        }
        if (before.revision() != command.expectedVersion()) {
            throw new NotificationCommandException("STALE_REVISION",
                "Recipient exception revision does not match the expected version");
        }
        if (!"OPEN".equals(before.status()) && !"IN_REVIEW".equals(before.status())) {
            throw new NotificationCommandException("CONFLICT",
                "Recipient exception is no longer repairable");
        }
        validateAction(command.action(), before);

        String nextStatus;
        switch (command.action()) {
            case RE_RESOLVE -> {
                RecipientResolutionService.ReResolveResult result = resolutions.reResolve(before.id());
                nextStatus = result.state() == com.vistay.notification.domain.recipient.ResolutionState.RESOLVED
                    ? "RESOLVED" : "IN_REVIEW";
            }
            case SUPPRESS -> nextStatus = "IGNORED";
            case CANCEL -> nextStatus = "RESOLVED";
            case ROUTE_ALTERNATE -> throw new NotificationCommandException("DEPENDENCY_UNAVAILABLE",
                "Alternate recipient routing is not configured");
            default -> throw new NotificationCommandException("VALIDATION_ERROR",
                "Recipient exception action is invalid");
        }

        int updated = jdbc.sql("""
            UPDATE notification_recipient_exceptions
            SET status = :status, resolution_action = :action,
                revision = revision + 1,
                resolved_at = CASE WHEN :terminal THEN now() ELSE NULL END,
                updated_at = now(), correlation_id = :correlationId
            WHERE id = :exceptionId AND revision = :expectedVersion
            """)
            .param("status", nextStatus)
            .param("action", command.action().name())
            .param("terminal", "RESOLVED".equals(nextStatus) || "IGNORED".equals(nextStatus))
            .param("correlationId", command.idempotencyKey())
            .param("exceptionId", before.id())
            .param("expectedVersion", command.expectedVersion())
            .update();
        if (updated != 1) {
            throw new NotificationCommandException("CONFLICT",
                "Recipient exception was changed concurrently");
        }
        writeHistory(before, command, nextStatus);
        writeAudit(before, command, nextStatus);
        writeOutbox(before, command, nextStatus);
        saveIdempotency(command, fingerprint);
        return get(before.id());
    }

    @Transactional(readOnly = true)
    public ExceptionView get(UUID exceptionId) {
        ExceptionRow row = jdbc.sql("""
            SELECT e.exception_sequence, e.id, e.notification_id, e.recipient_account_id,
                   e.recipient_kind, e.exception_type, e.status, e.reason_code,
                   e.resolution_action, e.destination_masked, e.contact_snapshot_revision,
                   e.revision, e.created_at, e.resolved_at, e.resolution_id
            FROM notification_recipient_exceptions e
            WHERE e.id = :exceptionId
            """)
            .param("exceptionId", exceptionId)
            .query(ExceptionRow.class)
            .list().stream().findFirst()
            .orElseThrow(() -> new NotificationCommandException("NOT_FOUND",
                "Recipient exception was not found"));
        return map(row);
    }

    private ExceptionRow loadForUpdate(UUID exceptionId) {
        return jdbc.sql("""
            SELECT e.exception_sequence, e.id, e.notification_id, e.recipient_account_id,
                   e.recipient_kind, e.exception_type, e.status, e.reason_code,
                   e.resolution_action, e.destination_masked, e.contact_snapshot_revision,
                   e.revision, e.created_at, e.resolved_at, e.resolution_id
            FROM notification_recipient_exceptions e
            WHERE e.id = :exceptionId
            FOR UPDATE
            """)
            .param("exceptionId", exceptionId)
            .query(ExceptionRow.class)
            .list().stream().findFirst().orElse(null);
    }

    private void validateAction(ResolutionAction action, ExceptionRow row) {
        if (action == ResolutionAction.SUPPRESS
                && !RecipientExceptionType.OPTIONAL_SUPPRESSED.name().equals(row.exceptionType())) {
            throw new NotificationCommandException("MANDATORY_CHANNEL_DISABLE",
                "Only optional recipient channels may be suppressed");
        }
        if (action == ResolutionAction.CANCEL && row.notificationId() != null) {
            Long required = jdbc.sql("""
                SELECT count(*)
                FROM notification_deliveries
                WHERE notification_id = :notificationId AND required = true
                  AND status <> 'CANCELLED'
                """)
                .param("notificationId", row.notificationId())
                .query(Long.class).single();
            if (required > 0) {
                throw new NotificationCommandException("MANDATORY_CHANNEL_DISABLE",
                    "Required notification channels cannot be cancelled");
            }
        }
    }

    private void writeHistory(ExceptionRow before, ResolveCommand command, String nextStatus) {
        jdbc.sql("""
            INSERT INTO notification_recipient_resolution_history(
                exception_id, resolution_id, from_status, to_status, action,
                reason, expected_revision, actor_account_type, actor_account_id,
                idempotency_key, safe_metadata
            ) VALUES (:exceptionId, :resolutionId, :fromStatus, :toStatus, :action,
                :reason, :expectedVersion, :actorType, :actorId, :idempotencyKey,
                CAST(:metadata AS jsonb))
            """)
            .param("exceptionId", before.id())
            .param("resolutionId", before.resolutionId())
            .param("fromStatus", before.status())
            .param("toStatus", nextStatus)
            .param("action", command.action().name())
            .param("reason", command.reason())
            .param("expectedVersion", command.expectedVersion())
            .param("actorType", command.actorAccountType())
            .param("actorId", command.actorAccountId())
            .param("idempotencyKey", command.idempotencyKey())
            .param("metadata", json(Map.of("fieldGroup", "INTERNAL_AUDIT")))
            .update();
    }

    private void writeAudit(ExceptionRow before, ResolveCommand command, String nextStatus) {
        jdbc.sql("""
            INSERT INTO audit_events(action, aggregate_type, aggregate_id, changes, correlation_id)
            VALUES ('NOTIFICATION_RECIPIENT_EXCEPTION_RESOLVED',
                'NotificationRecipientException', :exceptionId, CAST(:changes AS jsonb),
                :correlationId)
            """)
            .param("exceptionId", before.id())
            .param("changes", json(Map.of(
                "status", nextStatus,
                "action", command.action().name(),
                "reason", command.reason(),
                "expectedVersion", command.expectedVersion())))
            .param("correlationId", command.idempotencyKey())
            .update();
    }

    private void writeOutbox(ExceptionRow before, ResolveCommand command, String nextStatus) {
        jdbc.sql("""
            INSERT INTO outbox_events(
                aggregate_type, aggregate_id, aggregate_version, event_type,
                event_version, payload, correlation_id, causation_id
            ) VALUES ('NotificationRecipientException', :exceptionId, :revision,
                'vistay.notification.recipient-exception-recorded', 1,
                CAST(:payload AS jsonb), :correlationId, :causationId)
            """)
            .param("exceptionId", before.id())
            .param("revision", before.revision() + 1)
            .param("payload", json(Map.of(
                "exceptionId", before.id().toString(),
                "status", nextStatus,
                "action", command.action().name())))
            .param("correlationId", command.idempotencyKey())
            .param("causationId", before.resolutionId().toString())
            .update();
    }

    private IdempotencyRow findIdempotency(String key) {
        return jdbc.sql("""
            SELECT request_fingerprint, result
            FROM notification_command_idempotency
            WHERE operation = :operation AND idempotency_key = :idempotencyKey
            """)
            .param("operation", RESOLVE_OPERATION)
            .param("idempotencyKey", key)
            .query(IdempotencyRow.class).list().stream().findFirst().orElse(null);
    }

    private void saveIdempotency(ResolveCommand command, String fingerprint) {
        jdbc.sql("""
            INSERT INTO notification_command_idempotency(
                operation, idempotency_key, actor_account_type, actor_user_id,
                request_fingerprint, result
            ) VALUES (:operation, :idempotencyKey, :actorType, :actorId,
                :fingerprint, CAST(:result AS jsonb))
            ON CONFLICT (operation, idempotency_key) DO NOTHING
            """)
            .param("operation", RESOLVE_OPERATION)
            .param("idempotencyKey", command.idempotencyKey())
            .param("actorType", command.actorAccountType())
            .param("actorId", command.actorAccountId())
            .param("fingerprint", fingerprint)
            .param("result", json(Map.of("exceptionId", command.exceptionId().toString())))
            .update();
    }

    private ExceptionView map(ExceptionRow row) {
        return new ExceptionView(row.id(), row.notificationId(), row.recipientAccountId(),
            row.recipientKind(), row.exceptionType(), row.status(), row.reasonCode(),
            row.resolutionAction(), row.destinationMasked(), row.contactSnapshotRevision(),
            row.revision(), row.createdAt() == null ? null : row.createdAt().toString(),
            row.resolvedAt() == null ? null : row.resolvedAt().toString());
    }

    private static void validate(ResolveCommand command) {
        if (command == null || command.exceptionId() == null || command.actorAccountId() == null
                || command.action() == null || command.expectedVersion() < 1
                || command.reason() == null || command.reason().isBlank()
                || command.reason().length() > 1000
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 180) {
            throw new NotificationCommandException("VALIDATION_ERROR",
                "Recipient exception command is invalid");
        }
    }

    private static int boundedLimit(int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new NotificationCommandException("VALIDATION_ERROR",
                "Recipient exception limit must be between 1 and 100");
        }
        return limit;
    }

    private static <E extends Enum<E>> String enumValue(String value, Class<E> type, String field) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Enum.valueOf(type, value.trim().toUpperCase()).name();
        } catch (IllegalArgumentException invalid) {
            throw new NotificationCommandException("VALIDATION_ERROR", field + " is invalid");
        }
    }

    private static String json(ObjectMapper mapper, Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception failure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH",
                "Recipient exception evidence cannot be serialized");
        }
    }

    private String json(Object value) {
        return json(json, value);
    }

    public record ExceptionQuery(String afterCursor, int limit, String status, String exceptionType) {}

    public record ExceptionConnection(List<ExceptionView> nodes, PageInfo pageInfo) {}

    public record PageInfo(
            boolean hasNextPage,
            Long endCursor,
            Long startCursor,
            String cursorState,
            long retainedFrom,
            Long reconcileFrom) {}

    public record ExceptionView(
            UUID id,
            UUID notificationId,
            UUID recipientAccountId,
            String recipientKind,
            String exceptionType,
            String status,
            String reasonCode,
            String resolutionAction,
            String destinationMasked,
            Long contactSnapshotRevision,
            int revision,
            String createdAt,
            String resolvedAt) {}

    public record ResolveCommand(
            UUID exceptionId,
            ResolutionAction action,
            int expectedVersion,
            String reason,
            String idempotencyKey,
            String actorAccountType,
            UUID actorAccountId) {}

    private record ExceptionRow(
            long exceptionSequence,
            UUID id,
            UUID notificationId,
            UUID recipientAccountId,
            String recipientKind,
            String exceptionType,
            String status,
            String reasonCode,
            String resolutionAction,
            String destinationMasked,
            Long contactSnapshotRevision,
            int revision,
            OffsetDateTime createdAt,
            OffsetDateTime resolvedAt,
            UUID resolutionId) {}

    private record IdempotencyRow(String requestFingerprint, String result) {}
}
