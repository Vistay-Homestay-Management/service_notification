package com.vistay.notification.adapter.in.graphql;

import com.vistay.notification.application.NotificationAuthorizationService;
import com.vistay.notification.application.RecipientExceptionService;
import com.vistay.notification.domain.recipient.ResolutionAction;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;

/** Restricted GraphQL queue for redacted recipient-resolution exceptions. */
@Controller
public class RecipientExceptionController {
    private final NotificationAuthorizationService authorization;
    private final RecipientExceptionService exceptions;

    public RecipientExceptionController(
            NotificationAuthorizationService authorization,
            RecipientExceptionService exceptions) {
        this.authorization = authorization;
        this.exceptions = exceptions;
    }

    @QueryMapping
    public RecipientExceptionConnection notificationRecipientExceptions(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        authorization.requireOperator(jwt, "notifications:recipient-exception:read");
        RecipientExceptionService.ExceptionConnection result = exceptions.list(
            new RecipientExceptionService.ExceptionQuery(
                string(input, "afterCursor"), integer(input, "limit", 25),
                string(input, "status"), string(input, "exceptionType")));
        return new RecipientExceptionConnection(result.nodes().stream().map(this::map).toList(),
            map(result.pageInfo()));
    }

    @MutationMapping
    public RecipientExceptionView resolveNotificationRecipientException(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var actor = authorization.requireOperator(jwt,
            "notifications:recipient-exception:repair");
        ResolutionAction action;
        try {
            action = ResolutionAction.valueOf(requiredString(input, "action").toUpperCase());
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("action is invalid");
        }
        return map(exceptions.resolve(new RecipientExceptionService.ResolveCommand(
            UUID.fromString(requiredString(input, "exceptionId")), action,
            integer(input, "expectedVersion", Integer.MIN_VALUE),
            requiredString(input, "reason"), requiredString(input, "idempotencyKey"),
            actor.accountType(), actor.accountId())));
    }

    private RecipientExceptionView map(RecipientExceptionService.ExceptionView view) {
        return new RecipientExceptionView(view.id(), view.notificationId(), view.recipientAccountId(),
            view.recipientKind(), view.exceptionType(), "IN_REVIEW".equals(view.status()) ? "OPEN" : view.status(), view.reasonCode(),
            view.resolutionAction(), view.destinationMasked(), view.contactSnapshotRevision(),
            view.revision(), view.createdAt(), view.resolvedAt());
    }

    private static NotificationPageInfo map(RecipientExceptionService.PageInfo pageInfo) {
        return new NotificationPageInfo(pageInfo.hasNextPage(), cursor(pageInfo.endCursor()),
            cursor(pageInfo.startCursor()), pageInfo.cursorState(), cursor(pageInfo.retainedFrom()),
            cursor(pageInfo.reconcileFrom()));
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

    public record RecipientExceptionConnection(
            List<RecipientExceptionView> nodes,
            NotificationPageInfo pageInfo) {}

    public record NotificationPageInfo(
            boolean hasNextPage,
            String endCursor,
            String startCursor,
            String cursorState,
            String retainedFrom,
            String reconcileFrom) {}

    public record RecipientExceptionView(
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
}
