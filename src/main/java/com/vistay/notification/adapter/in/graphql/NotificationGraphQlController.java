package com.vistay.notification.adapter.in.graphql;

import com.vistay.notification.application.NotificationAuthorizationService;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
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

    public NotificationGraphQlController(
            JdbcClient jdbc,
            NotificationAuthorizationService authorization) {
        this.jdbc = jdbc;
        this.authorization = authorization;
    }

    @SubscriptionMapping
    public Flux<NotificationStreamEvent> notificationStream(
        @Argument String afterCursor,
        @AuthenticationPrincipal Jwt jwt
    ) {
        var recipient = authorization.requireOwnNotifications(jwt);
        var currentCursor = new AtomicLong(cursor(afterCursor));
        return Flux.interval(Duration.ZERO, Duration.ofSeconds(2))
            .concatMap(ignored -> Flux.fromIterable(readAfter(recipient, currentCursor.get())))
            .doOnNext(notification -> currentCursor.set(notification.eventSequence()))
            .map(notification -> new NotificationStreamEvent(
                Long.toString(notification.eventSequence()),
                toApi(notification)
            ));
    }

    private List<NotificationRow> readAfter(
            NotificationAuthorizationService.AuthorizedRecipient recipient,
            long cursor) {
        return jdbc.sql("""
            SELECT n.event_sequence, n.id, n.notification_type, n.title, n.body,
                   n.category, n.created_at, n.read_at,
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
        return new Notification(
            row.id(),
            category(row.category(), row.notificationType()),
            row.title(),
            row.body(),
            channels(row.channelsCsv()),
            row.createdAt().toString(),
            row.readAt() == null ? null : row.readAt().toString()
        );
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

    record NotificationRow(String category, long eventSequence, UUID id, String notificationType,
        String title, String body, String channelsCsv, OffsetDateTime createdAt,
        OffsetDateTime readAt) {
        NotificationRow(long eventSequence, UUID id, String notificationType, String title,
            String body, OffsetDateTime createdAt, OffsetDateTime readAt) {
            this(null, eventSequence, id, notificationType, title, body, "IN_APP", createdAt, readAt);
        }
    }
    public record NotificationStreamEvent(String cursor, Notification notification) {}
    public record Notification(UUID id, String category, String title, String body,
        List<String> channels, String createdAt, String readAt) {}
}
