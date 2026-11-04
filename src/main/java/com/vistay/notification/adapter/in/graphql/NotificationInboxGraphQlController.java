package com.vistay.notification.adapter.in.graphql;

import com.vistay.notification.application.NotificationAuthorizationService;
import com.vistay.notification.application.NotificationInboxService;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;

/** GraphQL boundary for recipient-owned inbox reads and durable read receipts. */
@Controller
public class NotificationInboxGraphQlController {
    private final NotificationAuthorizationService authorization;
    private final NotificationInboxService inbox;

    public NotificationInboxGraphQlController(
            NotificationAuthorizationService authorization,
            NotificationInboxService inbox) {
        this.authorization = authorization;
        this.inbox = inbox;
    }

    @QueryMapping
    public NotificationConnection notificationInbox(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var recipient = authorization.requireOwnNotifications(jwt);
        NotificationInboxService.InboxConnection result = inbox.inbox(recipient,
            new NotificationInboxService.InboxQuery(
                string(input, "afterCursor"), integer(input, "limit", 25),
                booleanValue(input, "unreadOnly"), string(input, "category")));
        return new NotificationConnection(
            result.nodes().stream().map(this::map).toList(),
            map(result.pageInfo()),
            map(result.unread()));
    }

    @QueryMapping
    public UnreadNotificationCount unreadNotificationCount(@AuthenticationPrincipal Jwt jwt) {
        var recipient = authorization.requireOwnNotifications(jwt);
        return map(inbox.unreadCount(recipient));
    }

    @MutationMapping
    public NotificationGraphQlController.Notification markNotificationRead(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var recipient = authorization.requireOwnNotifications(jwt);
        return map(inbox.markRead(new NotificationInboxService.MarkReadCommand(
            recipient,
            UUID.fromString(requiredString(input, "notificationId")),
            integer(input, "expectedVersion", Integer.MIN_VALUE),
            requiredString(input, "idempotencyKey"))));
    }

    @MutationMapping
    public MarkAllNotificationsReadPayload markAllNotificationsRead(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var recipient = authorization.requireOwnNotifications(jwt);
        NotificationInboxService.MarkAllResult result = inbox.markAllRead(
            new NotificationInboxService.MarkAllCommand(
                recipient, requiredString(input, "throughCursor"),
                requiredString(input, "idempotencyKey")));
        return new MarkAllNotificationsReadPayload(result.markedCount(),
            Long.toString(result.throughCursor()), result.inboxRevision());
    }

    private NotificationGraphQlController.Notification map(NotificationInboxService.NotificationView view) {
        NotificationGraphQlController.NotificationReference reference = view.reference() == null
            ? null
            : new NotificationGraphQlController.NotificationReference(
                view.reference().type(), view.reference().id(), view.reference().actionUrl());
        return new NotificationGraphQlController.Notification(
            view.id(), view.category(), view.title(), view.body(), view.channels(),
            view.createdAt(), view.readAt(), view.notificationType(), view.priority(), reference,
            view.templateKey(), view.templateRevision(), view.policyRevision(), view.inboxRevision(),
            view.requiredChannels(), view.optionalChannels(), view.version());
    }

    private static NotificationPageInfo map(NotificationInboxService.PageInfo pageInfo) {
        return new NotificationPageInfo(pageInfo.hasNextPage(), cursor(pageInfo.endCursor()),
            cursor(pageInfo.startCursor()), pageInfo.cursorState(), cursor(pageInfo.retainedFrom()),
            cursor(pageInfo.reconcileFrom()));
    }

    private static UnreadNotificationCount map(NotificationInboxService.UnreadCount unread) {
        return new UnreadNotificationCount(unread.count(), Long.toString(unread.asOfCursor()),
            unread.inboxRevision());
    }

    private static String cursor(Long value) {
        return value == null ? null : Long.toString(value);
    }

    private static String cursor(long value) {
        return Long.toString(value);
    }

    private static String requiredString(Map<String, Object> input, String field) {
        String value = string(input, field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static String string(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        return value == null ? null : String.valueOf(value);
    }

    private static int integer(Map<String, Object> input, String field, int defaultValue) {
        Object value = input == null ? null : input.get(field);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private static boolean booleanValue(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        return value instanceof Boolean bool && bool;
    }

    public record NotificationConnection(
            List<NotificationGraphQlController.Notification> nodes,
            NotificationPageInfo pageInfo,
            UnreadNotificationCount unread) {}

    public record NotificationPageInfo(
            boolean hasNextPage,
            String endCursor,
            String startCursor,
            String cursorState,
            String retainedFrom,
            String reconcileFrom) {}

    public record UnreadNotificationCount(int count, String asOfCursor, int inboxRevision) {}

    public record MarkAllNotificationsReadPayload(
            int markedCount,
            String throughCursor,
            int inboxRevision) {}
}
