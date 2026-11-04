package com.vistay.notification.application;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Owns the durable recipient inbox boundary. Every read is scoped by the
 * authenticated recipient, and every state-changing command is idempotent.
 */
@Service
public class NotificationInboxService {
    private static final String MARK_READ = "notification.inbox.mark-read";
    private static final String MARK_ALL_READ = "notification.inbox.mark-all-read";
    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_ACTIVE_CONNECTIONS = 3;

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public NotificationInboxService(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public InboxConnection inbox(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            InboxQuery query) {
        CursorWatermark watermark = watermark(recipient);
        long requestedCursor = query.afterCursor() == null
            ? Math.max(0, watermark.retainedFrom() - 1)
            : parseCursor(query.afterCursor());
        if (query.afterCursor() != null) {
            if (requestedCursor < watermark.retainedFrom()) {
                return new InboxConnection(List.of(),
                    new PageInfo(false, null, null, "EXPIRED", watermark.retainedFrom(),
                        watermark.retainedFrom()), unreadCount(recipient, watermark));
            }
            if (requestedCursor > watermark.latestCursor()) {
                return new InboxConnection(List.of(),
                    new PageInfo(false, null, null, "INVALID", watermark.retainedFrom(), null),
                    unreadCount(recipient, watermark));
            }
        }

        int limit = boundedLimit(query.limit());
        String category = normalizeCategory(query.category());
        StringBuilder sql = new StringBuilder("""
            SELECT n.event_sequence, n.id, n.notification_type, n.title, n.body,
                   n.category, n.priority, n.template_key, n.template_revision,
                   n.policy_revision, n.inbox_revision, n.notification_revision,
                   n.reference_type, n.reference_id, n.action_url, n.created_at,
                   n.read_at,
                   COALESCE((
                       SELECT string_agg(d.channel, ',' ORDER BY d.channel)
                       FROM notification_deliveries d
                       WHERE d.notification_id = n.id
                   ), 'IN_APP') AS channels_csv,
                   n.read_revision
            FROM notifications n
            WHERE n.recipient_account_type = :accountType
              AND n.recipient_user_id = :accountId
              AND n.event_sequence > :cursor
              AND n.status <> 'CANCELLED'
            """);
        if (query.unreadOnly()) {
            sql.append(" AND n.read_at IS NULL\n");
        }
        if (category != null) {
            sql.append(" AND n.category = :category\n");
        }
        sql.append(" ORDER BY n.event_sequence LIMIT :limitPlusOne");

        var statement = jdbc.sql(sql.toString())
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .param("cursor", requestedCursor)
            .param("limitPlusOne", limit + 1);
        if (category != null) {
            statement = statement.param("category", category);
        }
        List<NotificationRow> rows = statement.query(NotificationRow.class).list();
        boolean hasNext = rows.size() > limit;
        if (hasNext) {
            rows = new ArrayList<>(rows.subList(0, limit));
        }
        List<NotificationView> nodes = rows.stream().map(this::map).toList();
        Long start = rows.isEmpty() ? null : rows.get(0).eventSequence();
        Long end = rows.isEmpty() ? null : rows.get(rows.size() - 1).eventSequence();
        return new InboxConnection(nodes,
            new PageInfo(hasNext, end, start, "ACTIVE", watermark.retainedFrom(), null),
            unreadCount(recipient, watermark));
    }

    @Transactional
    public UnreadCount unreadCount(NotificationAuthorizationService.AuthorizedRecipient recipient) {
        return unreadCount(recipient, watermark(recipient));
    }

    private UnreadCount unreadCount(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            CursorWatermark watermark) {
        Long count = jdbc.sql("""
            SELECT count(*)
            FROM notifications
            WHERE recipient_account_type = :accountType
              AND recipient_user_id = :accountId
              AND read_at IS NULL
              AND status <> 'CANCELLED'
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .query(Long.class)
            .single();
        int inboxRevision = safeInt(watermark.latestCursor());
        return new UnreadCount(safeInt(count), watermark.latestCursor(), inboxRevision);
    }

    @Transactional
    public NotificationView markRead(MarkReadCommand command) {
        validateIdempotencyKey(command.idempotencyKey());
        String fingerprint = DomainEventNotificationService.fingerprint(
            MARK_READ, command.recipient().accountType(), command.recipient().accountId().toString(),
            command.notificationId().toString(), Integer.toString(command.expectedVersion()));
        IdempotencyRow previous = findIdempotency(MARK_READ, command.idempotencyKey());
        if (previous != null) {
            ensureSameRequest(previous, fingerprint);
            return load(command.recipient(), command.notificationId(), false);
        }

        NotificationRow before = loadRow(command.recipient(), command.notificationId(), true);
        if (before == null) {
            throw new NotificationCommandException("NOT_FOUND", "Notification was not found");
        }
        if (before.notificationRevision() != command.expectedVersion()) {
            throw new NotificationCommandException("STALE_REVISION",
                "Notification revision does not match the expected version");
        }

        boolean changed = before.readAt() == null;
        if (changed) {
            int updated = jdbc.sql("""
                UPDATE notifications
                SET read_at = now(), read_revision = read_revision + 1,
                    read_correlation_id = :correlationId, updated_at = now()
                WHERE id = :notificationId
                  AND recipient_account_type = :accountType
                  AND recipient_user_id = :accountId
                  AND notification_revision = :expectedVersion
                  AND read_at IS NULL
                """)
                .param("correlationId", command.idempotencyKey())
                .param("notificationId", command.notificationId())
                .param("accountType", command.recipient().accountType())
                .param("accountId", command.recipient().accountId())
                .param("expectedVersion", command.expectedVersion())
                .update();
            if (updated != 1) {
                throw new NotificationCommandException("CONFLICT",
                    "Notification was changed concurrently");
            }
            NotificationRow after = loadRow(command.recipient(), command.notificationId(), true);
            insertReceipt(after, command);
            writeReadAudit(after, command);
            writeReadOutbox(after, command);
        } else {
            insertReceiptIfMissing(before, command);
        }

        NotificationView result = load(command.recipient(), command.notificationId(), false);
        saveIdempotency(MARK_READ, command, fingerprint,
            json(Map.of("notificationId", command.notificationId().toString())));
        return result;
    }

    @Transactional
    public MarkAllResult markAllRead(MarkAllCommand command) {
        validateIdempotencyKey(command.idempotencyKey());
        long throughCursor = parseCursor(command.throughCursor());
        String fingerprint = DomainEventNotificationService.fingerprint(
            MARK_ALL_READ, command.recipient().accountType(), command.recipient().accountId().toString(),
            Long.toString(throughCursor));
        IdempotencyRow previous = findIdempotency(MARK_ALL_READ, command.idempotencyKey());
        if (previous != null) {
            ensureSameRequest(previous, fingerprint);
            return parseMarkAllResult(previous.result());
        }
        CursorWatermark watermark = watermark(command.recipient());
        validateCursor(throughCursor, watermark);

        List<NotificationRow> unread = jdbc.sql("""
            SELECT n.event_sequence, n.id, n.notification_type, n.title, n.body,
                   n.category, n.priority, n.template_key, n.template_revision,
                   n.policy_revision, n.inbox_revision, n.notification_revision,
                   n.reference_type, n.reference_id, n.action_url, n.created_at,
                   n.read_at,
                   COALESCE((
                       SELECT string_agg(d.channel, ',' ORDER BY d.channel)
                       FROM notification_deliveries d
                       WHERE d.notification_id = n.id
                   ), 'IN_APP') AS channels_csv,
                   n.read_revision
            FROM notifications n
            WHERE n.recipient_account_type = :accountType
              AND n.recipient_user_id = :accountId
              AND n.event_sequence <= :throughCursor
              AND n.read_at IS NULL
              AND n.status <> 'CANCELLED'
            ORDER BY n.event_sequence
            FOR UPDATE
            """)
            .param("accountType", command.recipient().accountType())
            .param("accountId", command.recipient().accountId())
            .param("throughCursor", throughCursor)
            .query(NotificationRow.class)
            .list();

        for (NotificationRow row : unread) {
            jdbc.sql("""
                UPDATE notifications
                SET read_at = now(), read_revision = read_revision + 1,
                    read_correlation_id = :correlationId, updated_at = now()
                WHERE id = :notificationId AND read_at IS NULL
                """)
                .param("correlationId", command.idempotencyKey())
                .param("notificationId", row.id())
                .update();
            NotificationRow after = loadRow(command.recipient(), row.id(), true);
            insertReceipt(after, command.idempotencyKey(), command.recipient());
        }
        if (!unread.isEmpty()) {
            writeMarkAllAudit(command, unread.size());
            writeMarkAllOutbox(command, throughCursor, unread.size());
        }

        MarkAllResult result = new MarkAllResult(unread.size(), throughCursor,
            safeInt(watermark.latestCursor()));
        saveIdempotency(MARK_ALL_READ, command, fingerprint, json(Map.of(
            "markedCount", result.markedCount(),
            "throughCursor", result.throughCursor(),
            "inboxRevision", result.inboxRevision())));
        return result;
    }

    /** Validates a supplied cursor without exposing another recipient's state. */
    @Transactional
    public void validateCursor(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            long cursor) {
        validateCursor(cursor, watermark(recipient));
    }

    @Transactional
    public StreamSession openStream(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            long afterCursor,
            boolean cursorSupplied) {
        CursorWatermark watermark = watermark(recipient);
        if (cursorSupplied) {
            validateCursor(afterCursor, watermark);
        }
        closeStaleStreams(recipient);
        Long active = jdbc.sql("""
            SELECT count(*)
            FROM notification_stream_connections
            WHERE recipient_account_type = :accountType
              AND recipient_user_id = :accountId
              AND state = 'ACTIVE'
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .query(Long.class)
            .single();
        if (active >= MAX_ACTIVE_CONNECTIONS) {
            throw new NotificationCommandException("CONNECTION_LIMIT",
                "Notification stream connection limit reached");
        }
        UUID id = jdbc.sql("""
            INSERT INTO notification_stream_connections(
                recipient_account_type, recipient_user_id, after_cursor,
                max_duration_seconds
            ) VALUES (:accountType, :accountId, :afterCursor, 1800)
            RETURNING id
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .param("afterCursor", afterCursor)
            .query(UUID.class)
            .single();
        return new StreamSession(id, recipient, afterCursor);
    }

    private void closeStaleStreams(
            NotificationAuthorizationService.AuthorizedRecipient recipient) {
        jdbc.sql("""
            UPDATE notification_stream_connections
            SET state = CASE WHEN opened_at + (max_duration_seconds * interval '1 second') <= now()
                             THEN 'EXPIRED' ELSE 'CLOSED' END,
                closed_at = now(), close_reason = 'STALE_HEARTBEAT'
            WHERE recipient_account_type = :accountType
              AND recipient_user_id = :accountId
              AND state = 'ACTIVE'
              AND last_heartbeat_at < now() - interval '30 seconds'
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .update();
    }

    @Transactional
    public void heartbeat(StreamSession session, long cursor, int emittedFrames) {
        jdbc.sql("""
            UPDATE notification_stream_connections
            SET after_cursor = GREATEST(after_cursor, :cursor),
                emitted_frames = emitted_frames + :emittedFrames,
                last_heartbeat_at = now()
            WHERE id = :id AND state = 'ACTIVE'
            """)
            .param("cursor", cursor)
            .param("emittedFrames", Math.max(0, emittedFrames))
            .param("id", session.id())
            .update();
    }

    @Transactional
    public void closeStream(StreamSession session, String reason) {
        jdbc.sql("""
            UPDATE notification_stream_connections
            SET state = CASE WHEN opened_at + (max_duration_seconds * interval '1 second') <= now()
                             THEN 'EXPIRED' ELSE 'CLOSED' END,
                closed_at = now(), close_reason = :reason, last_heartbeat_at = now()
            WHERE id = :id AND state = 'ACTIVE'
            """)
            .param("reason", boundedReason(reason))
            .param("id", session.id())
            .update();
    }

    public static long parseCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        if (!cursor.matches("0|[1-9][0-9]*")) {
            throw new NotificationCommandException("INVALID_CURSOR",
                "Cursor must be a canonical non-negative decimal");
        }
        try {
            return Long.parseLong(cursor);
        } catch (NumberFormatException invalid) {
            throw new NotificationCommandException("INVALID_CURSOR",
                "Cursor must be a canonical non-negative decimal");
        }
    }

    private CursorWatermark watermark(
            NotificationAuthorizationService.AuthorizedRecipient recipient) {
        jdbc.sql("""
            INSERT INTO notification_cursor_watermarks(
                recipient_account_type, recipient_user_id, latest_cursor
            )
            SELECT :accountType, :accountId, COALESCE(max(event_sequence), 0)
            FROM notifications
            WHERE recipient_account_type = :accountType
              AND recipient_user_id = :accountId
            ON CONFLICT (recipient_account_type, recipient_user_id) DO UPDATE
            SET latest_cursor = GREATEST(notification_cursor_watermarks.latest_cursor,
                                         EXCLUDED.latest_cursor),
                updated_at = now()
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .update();
        return jdbc.sql("""
            SELECT retained_from, latest_cursor, revision
            FROM notification_cursor_watermarks
            WHERE recipient_account_type = :accountType
              AND recipient_user_id = :accountId
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .query(CursorWatermark.class)
            .single();
    }

    private static void validateCursor(long cursor, CursorWatermark watermark) {
        if (cursor < watermark.retainedFrom()) {
            throw new NotificationCommandException("CURSOR_EXPIRED",
                "Cursor expired; reconcile from the retained inbox watermark");
        }
        if (cursor > watermark.latestCursor()) {
            throw new NotificationCommandException("INVALID_CURSOR",
                "Cursor is ahead of the authenticated inbox");
        }
    }

    private NotificationView load(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            UUID notificationId,
            boolean forUpdate) {
        NotificationRow row = loadRow(recipient, notificationId, forUpdate);
        if (row == null) {
            throw new NotificationCommandException("NOT_FOUND", "Notification was not found");
        }
        return map(row);
    }

    private NotificationRow loadRow(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            UUID notificationId,
            boolean forUpdate) {
        String lock = forUpdate ? " FOR UPDATE" : "";
        return jdbc.sql("""
            SELECT n.event_sequence, n.id, n.notification_type, n.title, n.body,
                   n.category, n.priority, n.template_key, n.template_revision,
                   n.policy_revision, n.inbox_revision, n.notification_revision,
                   n.reference_type, n.reference_id, n.action_url, n.created_at,
                   n.read_at,
                   COALESCE((
                       SELECT string_agg(d.channel, ',' ORDER BY d.channel)
                       FROM notification_deliveries d
                       WHERE d.notification_id = n.id
                   ), 'IN_APP') AS channels_csv,
                   n.read_revision
            FROM notifications n
            WHERE n.id = :notificationId
              AND n.recipient_account_type = :accountType
              AND n.recipient_user_id = :accountId
              AND n.status <> 'CANCELLED'
            """ + lock)
            .param("notificationId", notificationId)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .query(NotificationRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private NotificationView map(NotificationRow row) {
        List<String> channels = channels(row.channelsCsv());
        List<String> required = channels.stream()
            .filter(channel -> "IN_APP".equals(channel) || "EMAIL".equals(channel))
            .toList();
        List<String> optional = channels.stream()
            .filter(channel -> !required.contains(channel))
            .toList();
        NotificationReference reference = row.referenceType() == null || row.referenceId() == null
            ? null
            : new NotificationReference(row.referenceType(), row.referenceId(), safeActionUrl(row.actionUrl()));
        return new NotificationView(
            row.id(), category(row.category(), row.notificationType()), row.title(), row.body(), channels,
            row.createdAt() == null ? null : row.createdAt().toString(),
            row.readAt() == null ? null : row.readAt().toString(), row.notificationType(),
            row.priority() == null ? "NORMAL" : row.priority(), reference, row.templateKey(),
            row.templateRevision(), row.policyRevision(), safeInt(row.inboxRevision() == null
                ? row.eventSequence() : row.inboxRevision()), required, optional,
            row.notificationRevision() == null ? 1 : row.notificationRevision());
    }

    private void insertReceipt(NotificationRow row, MarkReadCommand command) {
        insertReceipt(row, command.idempotencyKey(), command.recipient());
    }

    private void insertReceipt(
            NotificationRow row,
            String idempotencyKey,
            NotificationAuthorizationService.AuthorizedRecipient recipient) {
        jdbc.sql("""
            INSERT INTO notification_read_receipts(
                notification_id, recipient_account_type, recipient_user_id,
                read_at, revision, idempotency_key, correlation_id
            ) VALUES (:notificationId, :accountType, :accountId, COALESCE(:readAt, now()),
                :revision, :idempotencyKey, :correlationId)
            ON CONFLICT (notification_id) DO NOTHING
            """)
            .param("notificationId", row.id())
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .param("readAt", row.readAt())
            .param("revision", Math.max(1, row.readRevision() == null ? 1 : row.readRevision()))
            .param("idempotencyKey", idempotencyKey)
            .param("correlationId", idempotencyKey)
            .update();
    }

    private void insertReceiptIfMissing(
            NotificationRow row,
            MarkReadCommand command) {
        insertReceipt(row, command.idempotencyKey(), command.recipient());
    }

    private void writeReadAudit(NotificationRow row, MarkReadCommand command) {
        jdbc.sql("""
            INSERT INTO audit_events(action, aggregate_type, aggregate_id, changes, correlation_id)
            VALUES ('NOTIFICATION_READ', 'Notification', :notificationId,
                CAST(:changes AS jsonb), :correlationId)
            """)
            .param("notificationId", row.id())
            .param("changes", json(Map.of(
                "recipientAccountType", command.recipient().accountType(),
                "recipientAccountId", command.recipient().accountId().toString(),
                "readRevision", row.readRevision())))
            .param("correlationId", command.idempotencyKey())
            .update();
    }

    private void writeReadOutbox(NotificationRow row, MarkReadCommand command) {
        jdbc.sql("""
            INSERT INTO outbox_events(
                aggregate_type, aggregate_id, aggregate_version, event_type,
                event_version, payload, correlation_id, causation_id
            ) VALUES ('Notification', :notificationId, :aggregateVersion,
                'vistay.notification.notification-read', 1, CAST(:payload AS jsonb),
                :correlationId, :causationId)
            """)
            .param("notificationId", row.id())
            .param("aggregateVersion", row.eventSequence())
            .param("payload", json(Map.of(
                "notificationId", row.id().toString(),
                "inboxRevision", safeInt(row.inboxRevision() == null
                    ? row.eventSequence() : row.inboxRevision()),
                "readRevision", row.readRevision())))
            .param("correlationId", command.idempotencyKey())
            .param("causationId", command.idempotencyKey())
            .update();
    }

    private void writeMarkAllAudit(MarkAllCommand command, int count) {
        jdbc.sql("""
            INSERT INTO audit_events(action, aggregate_type, aggregate_id, changes, correlation_id)
            VALUES ('NOTIFICATIONS_MARKED_READ', 'NotificationInbox', :aggregateId,
                CAST(:changes AS jsonb), :correlationId)
            """)
            .param("aggregateId", command.recipient().accountId())
            .param("changes", json(Map.of(
                "recipientAccountType", command.recipient().accountType(),
                "recipientAccountId", command.recipient().accountId().toString(),
                "throughCursor", command.throughCursor(), "markedCount", count)))
            .param("correlationId", command.idempotencyKey())
            .update();
    }

    private void writeMarkAllOutbox(MarkAllCommand command, long throughCursor, int count) {
        jdbc.sql("""
            INSERT INTO outbox_events(
                aggregate_type, aggregate_id, aggregate_version, event_type,
                event_version, payload, correlation_id, causation_id
            ) VALUES ('NotificationInbox', :aggregateId, :aggregateVersion,
                'vistay.notification.notifications-read', 1, CAST(:payload AS jsonb),
                :correlationId, :causationId)
            """)
            .param("aggregateId", command.recipient().accountId())
            .param("aggregateVersion", throughCursor)
            .param("payload", json(Map.of(
                "recipientAccountType", command.recipient().accountType(),
                "recipientAccountId", command.recipient().accountId().toString(),
                "throughCursor", throughCursor, "markedCount", count)))
            .param("correlationId", command.idempotencyKey())
            .param("causationId", command.idempotencyKey())
            .update();
    }

    private IdempotencyRow findIdempotency(String operation, String key) {
        return jdbc.sql("""
            SELECT request_fingerprint, result
            FROM notification_command_idempotency
            WHERE operation = :operation AND idempotency_key = :idempotencyKey
            """)
            .param("operation", operation)
            .param("idempotencyKey", key)
            .query(IdempotencyRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private void saveIdempotency(
            String operation,
            Command command,
            String fingerprint,
            String result) {
        jdbc.sql("""
            INSERT INTO notification_command_idempotency(
                operation, idempotency_key, actor_account_type, actor_user_id,
                request_fingerprint, result
            ) VALUES (:operation, :idempotencyKey, :accountType, :accountId,
                :fingerprint, CAST(:result AS jsonb))
            ON CONFLICT (operation, idempotency_key) DO NOTHING
            """)
            .param("operation", operation)
            .param("idempotencyKey", command.idempotencyKey())
            .param("accountType", command.recipient().accountType())
            .param("accountId", command.recipient().accountId())
            .param("fingerprint", fingerprint)
            .param("result", result)
            .update();
    }

    private static void ensureSameRequest(IdempotencyRow previous, String fingerprint) {
        if (!fingerprint.equals(previous.requestFingerprint())) {
            throw new NotificationCommandException("IDEMPOTENCY_KEY_REUSED",
                "Idempotency key was already used for another request");
        }
    }

    private static MarkAllResult parseMarkAllResult(String result) {
        try {
            return new MarkAllResult(
                integerField(result, "markedCount"),
                longField(result, "throughCursor"),
                integerField(result, "inboxRevision"));
        } catch (RuntimeException invalid) {
            throw new NotificationCommandException("IDEMPOTENCY_RESULT_INVALID",
                "Stored notification command result is invalid");
        }
    }

    private static int integerField(String json, String field) {
        return Math.toIntExact(longField(json, field));
    }

    private static long longField(String json, String field) {
        String marker = "\"" + field + "\":";
        int start = json.indexOf(marker);
        if (start < 0) {
            throw new IllegalArgumentException("missing field");
        }
        start += marker.length();
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        return Long.parseLong(json.substring(start, end));
    }

    private static int boundedLimit(int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new NotificationCommandException("VALIDATION_ERROR",
                "Inbox limit must be between 1 and 100");
        }
        return limit;
    }

    private static String normalizeCategory(String category) {
        if (category == null || category.isBlank()) {
            return null;
        }
        return switch (category.trim().toUpperCase()) {
            case "WELCOME", "SECURITY", "BOOKING", "OPERATIONS", "FINANCE", "SYSTEM"
                -> category.trim().toUpperCase();
            default -> throw new NotificationCommandException("VALIDATION_ERROR",
                "Notification category is invalid");
        };
    }

    private static List<String> channels(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of("IN_APP");
        }
        return Arrays.stream(csv.split(","))
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .toList();
    }

    private static String category(String storedCategory, String notificationType) {
        if (storedCategory != null && !storedCategory.isBlank()) {
            return storedCategory;
        }
        return notificationType != null && notificationType.contains("SECURITY")
            ? "SECURITY" : "WELCOME";
    }

    private static String safeActionUrl(String actionUrl) {
        if (actionUrl == null || actionUrl.isBlank() || !actionUrl.startsWith("/")
                || actionUrl.startsWith("//") || actionUrl.contains("\\")) {
            return null;
        }
        return actionUrl;
    }

    private static int safeInt(long value) {
        return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.toIntExact(Math.max(0, value));
    }

    private static int safeInt(Long value) {
        return safeInt(value == null ? 0 : value);
    }

    private static String boundedReason(String reason) {
        if (reason == null || reason.isBlank()) {
            return "CLIENT_CLOSED";
        }
        return reason.length() > 100 ? reason.substring(0, 100) : reason;
    }

    private static void validateIdempotencyKey(String key) {
        if (key == null || key.isBlank() || key.length() > 180) {
            throw new NotificationCommandException("VALIDATION_ERROR",
                "Idempotency key is invalid");
        }
    }

    private String json(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception serializationFailure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH",
                "Notification command metadata cannot be serialized");
        }
    }

    public record InboxQuery(String afterCursor, int limit, boolean unreadOnly, String category) {}

    public record InboxConnection(List<NotificationView> nodes, PageInfo pageInfo, UnreadCount unread) {}

    public record PageInfo(
            boolean hasNextPage,
            Long endCursor,
            Long startCursor,
            String cursorState,
            long retainedFrom,
            Long reconcileFrom) {}

    public record UnreadCount(int count, long asOfCursor, int inboxRevision) {}

    public record NotificationReference(String type, UUID id, String actionUrl) {}

    public record NotificationView(
            UUID id,
            String category,
            String title,
            String body,
            List<String> channels,
            String createdAt,
            String readAt,
            String notificationType,
            String priority,
            NotificationReference reference,
            String templateKey,
            Integer templateRevision,
            Integer policyRevision,
            Integer inboxRevision,
            List<String> requiredChannels,
            List<String> optionalChannels,
            Integer version) {}

    public record MarkReadCommand(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            UUID notificationId,
            int expectedVersion,
            String idempotencyKey) implements Command {}

    public record MarkAllCommand(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            String throughCursor,
            String idempotencyKey) implements Command {}

    public record MarkAllResult(int markedCount, long throughCursor, int inboxRevision) {}

    public record StreamSession(
            UUID id,
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            long afterCursor) {}

    private record CursorWatermark(long retainedFrom, long latestCursor, int revision) {}

    private record IdempotencyRow(String requestFingerprint, String result) {}

    private record NotificationRow(
            long eventSequence,
            UUID id,
            String notificationType,
            String title,
            String body,
            String category,
            String priority,
            String templateKey,
            Integer templateRevision,
            Integer policyRevision,
            Long inboxRevision,
            Integer notificationRevision,
            String referenceType,
            UUID referenceId,
            String actionUrl,
            OffsetDateTime createdAt,
            OffsetDateTime readAt,
            String channelsCsv,
            Integer readRevision) {}

    private interface Command {
        NotificationAuthorizationService.AuthorizedRecipient recipient();
        String idempotencyKey();
    }
}
