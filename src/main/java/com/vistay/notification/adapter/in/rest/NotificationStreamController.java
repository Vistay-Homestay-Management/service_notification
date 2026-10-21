package com.vistay.notification.adapter.in.rest;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1/notifications")
public class NotificationStreamController {
    private static final long STREAM_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(30);
    private final JdbcClient jdbc;

    public NotificationStreamController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    SseEmitter stream(
        @AuthenticationPrincipal Jwt jwt,
        @RequestHeader(name = "Last-Event-ID", required = false) Long lastEventId
    ) {
        var accountId = UUID.fromString(jwt.getSubject());
        var accountType = jwt.getClaimAsString("account_type");
        var cursor = new AtomicLong(lastEventId == null ? 0 : Math.max(lastEventId, 0));
        var closed = new AtomicBoolean();
        var emitter = new SseEmitter(STREAM_TIMEOUT_MILLIS);
        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("notification-sse-", 0).factory()
        );

        Runnable close = () -> {
            if (closed.compareAndSet(false, true)) {
                poller.shutdownNow();
            }
        };
        emitter.onCompletion(close);
        emitter.onTimeout(() -> {
            close.run();
            emitter.complete();
        });
        emitter.onError(error -> close.run());

        poller.scheduleWithFixedDelay(() -> {
            if (closed.get()) {
                return;
            }
            try {
                var notifications = readAfter(accountType, accountId, cursor.get());
                for (var notification : notifications) {
                    emitter.send(SseEmitter.event()
                        .id(Long.toString(notification.eventSequence()))
                        .name("notification")
                        .data(notification, MediaType.APPLICATION_JSON));
                    cursor.set(notification.eventSequence());
                }
                if (notifications.isEmpty()) {
                    emitter.send(SseEmitter.event().comment("keepalive"));
                }
            } catch (IOException | RuntimeException failure) {
                close.run();
                emitter.completeWithError(failure);
            }
        }, 0, 10, TimeUnit.SECONDS);
        return emitter;
    }

    private List<NotificationEvent> readAfter(String type, UUID userId, long cursor) {
        return jdbc.sql("""
            SELECT event_sequence, id, notification_type, title, body, priority,
                   reference_type, reference_id, action_url, data, created_at
            FROM notifications
            WHERE recipient_account_type = :type
              AND recipient_user_id = :userId
              AND event_sequence > :cursor
              AND status <> 'CANCELLED'
            ORDER BY event_sequence
            LIMIT 100
            """)
            .param("type", type)
            .param("userId", userId)
            .param("cursor", cursor)
            .query(NotificationEvent.class)
            .list();
    }

    public record NotificationEvent(
        long eventSequence,
        UUID id,
        String notificationType,
        String title,
        String body,
        String priority,
        String referenceType,
        UUID referenceId,
        String actionUrl,
        String data,
        java.time.OffsetDateTime createdAt
    ) {}
}
