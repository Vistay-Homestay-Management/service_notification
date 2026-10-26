package com.vistay.notification.adapter.in.graphql;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

@Controller
public class NotificationGraphQlController {
    private final JdbcClient jdbc;

    public NotificationGraphQlController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @SubscriptionMapping
    @PreAuthorize("hasAuthority('SCOPE_notifications:read')")
    public Flux<NotificationStreamEvent> notificationStream(
        @Argument String afterCursor,
        @AuthenticationPrincipal Jwt jwt
    ) {
        var accountId = accountId(jwt);
        var currentCursor = new AtomicLong(cursor(afterCursor));
        return Flux.interval(Duration.ZERO, Duration.ofSeconds(2))
            .concatMap(ignored -> Flux.fromIterable(readAfter(accountId, currentCursor.get())))
            .doOnNext(notification -> currentCursor.set(notification.eventSequence()))
            .map(notification -> new NotificationStreamEvent(
                Long.toString(notification.eventSequence()),
                toApi(notification)
            ));
    }

    private List<NotificationRow> readAfter(UUID accountId, long cursor) {
        return jdbc.sql("""
            SELECT n.event_sequence, n.id, n.notification_type, n.title, n.body,
                   n.created_at, n.read_at
            FROM notifications n
            WHERE n.recipient_account_type = 'ADMIN'
              AND n.recipient_user_id = :accountId
              AND n.event_sequence > :cursor
              AND n.status <> 'CANCELLED'
            ORDER BY n.event_sequence
            LIMIT 100
            """)
            .param("accountId", accountId)
            .param("cursor", cursor)
            .query(NotificationRow.class)
            .list();
    }

    private static Notification toApi(NotificationRow row) {
        return new Notification(
            row.id(),
            category(row.notificationType()),
            row.title(),
            row.body(),
            List.of("IN_APP"),
            row.createdAt().toString(),
            row.readAt() == null ? null : row.readAt().toString()
        );
    }

    private static String category(String notificationType) {
        return notificationType != null && notificationType.contains("SECURITY")
            ? "SECURITY"
            : "WELCOME";
    }

    private static UUID accountId(Jwt jwt) {
        if (jwt == null) {
            throw new IllegalArgumentException("Authentication is required");
        }
        return UUID.fromString(jwt.getSubject());
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

    record NotificationRow(long eventSequence, UUID id, String notificationType, String title,
        String body, OffsetDateTime createdAt, OffsetDateTime readAt) {}
    public record NotificationStreamEvent(String cursor, Notification notification) {}
    public record Notification(UUID id, String category, String title, String body,
        List<String> channels, String createdAt, String readAt) {}
}
