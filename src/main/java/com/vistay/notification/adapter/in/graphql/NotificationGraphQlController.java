package com.vistay.notification.adapter.in.graphql;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

@Controller
public class NotificationGraphQlController {
    private final JdbcClient jdbc;

    public NotificationGraphQlController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @QueryMapping
    public List<Notification> notificationInbox(
        @Argument String afterCursor,
        @Argument Integer first,
        @AuthenticationPrincipal Jwt jwt
    ) {
        return readAfter(accountType(jwt), accountId(jwt), cursor(afterCursor), bounded(first));
    }

    @SubscriptionMapping
    public Flux<NotificationStreamEvent> notificationStream(
        @Argument String afterCursor,
        @AuthenticationPrincipal Jwt jwt
    ) {
        var accountType = accountType(jwt);
        var accountId = accountId(jwt);
        var currentCursor = new AtomicLong(cursor(afterCursor));
        return Flux.interval(Duration.ZERO, Duration.ofSeconds(2))
            .concatMap(ignored -> Flux.fromIterable(
                readAfter(accountType, accountId, currentCursor.get(), 100)
            ))
            .doOnNext(notification -> currentCursor.set(notification.eventSequence()))
            .map(notification -> new NotificationStreamEvent(
                notification.eventSequence(), notification
            ));
    }

    private List<Notification> readAfter(String type, UUID userId, long cursor, int limit) {
        return jdbc.sql("""
            SELECT event_sequence, id, notification_type, title, body, priority,
                   reference_type, reference_id, action_url, data::text AS data, created_at
            FROM notifications
            WHERE recipient_account_type = :type
              AND recipient_user_id = :userId
              AND event_sequence > :cursor
              AND status <> 'CANCELLED'
            ORDER BY event_sequence
            LIMIT :limit
            """)
            .param("type", type)
            .param("userId", userId)
            .param("cursor", cursor)
            .param("limit", limit)
            .query(Notification.class)
            .list();
    }

    private static UUID accountId(Jwt jwt) {
        if (jwt == null) {
            throw new IllegalArgumentException("Authentication is required");
        }
        return UUID.fromString(jwt.getSubject());
    }

    private static String accountType(Jwt jwt) {
        var type = jwt == null ? null : jwt.getClaimAsString("account_type");
        return type == null || type.isBlank() ? "ADMIN" : type;
    }

    private static long cursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        try {
            return Math.max(Long.parseLong(cursor), 0);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("afterCursor must be a non-negative decimal cursor");
        }
    }

    private static int bounded(Integer first) {
        return first == null ? 50 : Math.max(1, Math.min(first, 100));
    }

    public record NotificationStreamEvent(long cursor, Notification notification) {}
    public record Notification(long eventSequence, UUID id, String notificationType, String title,
        String body, String priority, String referenceType, UUID referenceId, String actionUrl,
        String data, OffsetDateTime createdAt) {}
}
