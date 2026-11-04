package com.vistay.notification.adapter.in.graphql;

import com.vistay.notification.application.NotificationAuthorizationService;
import com.vistay.notification.application.NotificationInboxService;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

@Controller
public class NotificationGraphQlController {
    private final JdbcClient jdbc;
    private final NotificationAuthorizationService authorization;
    private final NotificationInboxService inbox;
    private final Duration maxDuration;
    private final Duration pollInterval;

    public NotificationGraphQlController(
            JdbcClient jdbc,
            NotificationAuthorizationService authorization) {
        this(jdbc, authorization, null, Duration.ofMinutes(30), Duration.ofSeconds(2));
    }

    @Autowired
    public NotificationGraphQlController(
            JdbcClient jdbc,
            NotificationAuthorizationService authorization,
            NotificationInboxService inbox,
            @Value("${vistay.notifications.stream-max-duration:30m}") Duration maxDuration,
            @Value("${vistay.notifications.stream-poll-interval:2s}") Duration pollInterval) {
        this.jdbc = jdbc;
        this.authorization = authorization;
        this.inbox = inbox;
        this.maxDuration = maxDuration.isZero() || maxDuration.isNegative()
            ? Duration.ofMinutes(30) : maxDuration;
        this.pollInterval = pollInterval.isZero() || pollInterval.isNegative()
            ? Duration.ofSeconds(2) : pollInterval;
    }

    @SubscriptionMapping
    public Flux<NotificationStreamEvent> notificationStream(
        @Argument String afterCursor,
        @AuthenticationPrincipal Jwt jwt
    ) {
        var recipient = authorization.requireOwnNotifications(jwt);
        var currentCursor = new AtomicLong(cursor(afterCursor));
        boolean cursorSupplied = afterCursor != null && !afterCursor.isBlank();
        NotificationInboxService.StreamSession session = inbox == null ? null
            : inbox.openStream(recipient, currentCursor.get(), cursorSupplied);
        return Flux.interval(Duration.ZERO, pollInterval)
            .onBackpressureLatest()
            .doOnNext(tick -> {
                if (session != null) {
                    inbox.heartbeat(session, currentCursor.get(), 0);
                }
            })
            .concatMap(ignored -> Flux.fromIterable(readAfter(recipient, currentCursor.get())))
            .doOnNext(notification -> currentCursor.set(notification.eventSequence()))
            .map(notification -> new NotificationStreamEvent(
                Long.toString(notification.eventSequence()),
                toApi(notification), notification.eventVersion(),
                notification.notificationRevision(), "AT_LEAST_ONCE"
            ))
            .take(maxDuration)
            .doOnNext(event -> {
                if (session != null) {
                    inbox.heartbeat(session, currentCursor.get(), 1);
                }
            })
            .doFinally(signal -> {
                if (session != null) {
                    inbox.closeStream(session, signal.name());
                }
            });
    }

    private List<NotificationRow> readAfter(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            long cursor) {
        return jdbc.sql("""
            SELECT n.event_sequence, n.id, n.notification_type, n.title, n.body,
                   n.category, n.priority, n.template_key, n.template_revision,
                   n.policy_revision, n.inbox_revision, n.notification_revision,
                   n.source_event_version, n.reference_type, n.reference_id,
                   n.action_url, n.created_at, n.read_at, n.read_revision,
                   COALESCE((
                       SELECT string_agg(d.channel, ',' ORDER BY d.channel)
                       FROM notification_deliveries d
                       WHERE d.notification_id = n.id
                   ), 'IN_APP') AS channels_csv
            FROM notifications n
            WHERE n.recipient_account_type = :accountType
              AND n.recipient_user_id = :accountId
              AND n.event_sequence > :cursor
              AND n.status <> 'CANCELLED'
            ORDER BY n.event_sequence
            LIMIT 100
            """)
            .param("accountType", recipient.accountType())
            .param("accountId", recipient.accountId())
            .param("cursor", cursor)
            .query(NotificationRow.class)
            .list();
    }

    private static Notification toApi(NotificationRow row) {
        List<String> channels = channels(row.channelsCsv());
        List<String> requiredChannels = channels.stream()
            .filter(channel -> "IN_APP".equals(channel) || "EMAIL".equals(channel))
            .toList();
        List<String> optionalChannels = channels.stream()
            .filter(channel -> !requiredChannels.contains(channel))
            .toList();
        NotificationReference reference = row.referenceType() == null || row.referenceId() == null
            ? null : new NotificationReference(row.referenceType(), row.referenceId(), safeActionUrl(row.actionUrl()));
        return new Notification(
            row.id(),
            category(row.category(), row.notificationType()),
            row.title(),
            row.body(),
            channels(row.channelsCsv()),
            row.createdAt().toString(),
            row.readAt() == null ? null : row.readAt().toString(),
            row.notificationType(),
            row.priority(),
            reference,
            row.templateKey(),
            row.templateRevision(),
            row.policyRevision(),
            row.inboxRevision() == null ? safeInt(row.eventSequence()) : safeInt(row.inboxRevision()),
            requiredChannels,
            optionalChannels,
            row.notificationRevision() == null ? 1 : row.notificationRevision()
        );
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

    private static List<String> channels(String channelsCsv) {
        if (channelsCsv == null || channelsCsv.isBlank()) {
            return List.of("IN_APP");
        }
        return Arrays.stream(channelsCsv.split(","))
            .filter(channel -> !channel.isBlank())
            .toList();
    }

    private static String category(String storedCategory, String notificationType) {
        if (storedCategory != null && !storedCategory.isBlank()) {
            return storedCategory;
        }
        return notificationType != null && notificationType.contains("SECURITY")
            ? "SECURITY"
            : "WELCOME";
    }

    private static long cursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        if (!cursor.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException("afterCursor must be a canonical non-negative decimal cursor");
        }
        try {
            return Long.parseLong(cursor);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(
                "afterCursor must be a canonical non-negative decimal cursor", invalid);
        }
    }

    record NotificationRow(
        String category,
        long eventSequence,
        UUID id,
        String notificationType,
        String title,
        String body,
        String priority,
        String templateKey,
        Integer templateRevision,
        Integer policyRevision,
        Long inboxRevision,
        Integer notificationRevision,
        Integer eventVersion,
        String referenceType,
        UUID referenceId,
        String actionUrl,
        String channelsCsv,
        OffsetDateTime createdAt,
        OffsetDateTime readAt,
        Integer readRevision) {
        NotificationRow(long eventSequence, UUID id, String notificationType, String title,
            String body, OffsetDateTime createdAt, OffsetDateTime readAt) {
            this(null, eventSequence, id, notificationType, title, body, null, null, null, null,
                null, null, null, null, null, null, "IN_APP", createdAt, readAt, null);
        }
    }
    public record NotificationStreamEvent(
            String cursor,
            Notification notification,
            Integer eventVersion,
            Integer notificationRevision,
            String deliveryGuarantee) {
        public NotificationStreamEvent(String cursor, Notification notification) {
            this(cursor, notification, null, null, "AT_LEAST_ONCE");
        }
    }

    public record NotificationReference(String type, UUID id, String actionUrl) {}

    public record Notification(UUID id, String category, String title, String body,
        List<String> channels, String createdAt, String readAt,
        String notificationType, String priority, NotificationReference reference,
        String templateKey, Integer templateRevision, Integer policyRevision,
        Integer inboxRevision, List<String> requiredChannels,
        List<String> optionalChannels, Integer version) {
        public Notification(UUID id, String category, String title, String body,
                List<String> channels, String createdAt, String readAt) {
            this(id, category, title, body, channels, createdAt, readAt, null, null, null,
                null, null, null, null, List.of(), List.of(), null);
        }
    }
}
