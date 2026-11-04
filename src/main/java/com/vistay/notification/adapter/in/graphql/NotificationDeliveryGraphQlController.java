package com.vistay.notification.adapter.in.graphql;

import com.vistay.notification.application.NotificationAuthorizationService;
import com.vistay.notification.application.NotificationDeliveryRetryService;
import com.vistay.notification.application.NotificationDeliveryRetryService.RepairAction;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;

/** GraphQL operations for bounded delivery inspection and retry repair. */
@Controller
public class NotificationDeliveryGraphQlController {
    private final NotificationAuthorizationService authorization;
    private final NotificationDeliveryRetryService deliveries;

    public NotificationDeliveryGraphQlController(
            NotificationAuthorizationService authorization,
            NotificationDeliveryRetryService deliveries) {
        this.authorization = authorization;
        this.deliveries = deliveries;
    }

    @QueryMapping
    public DeliveryView notificationDelivery(
            @Argument String id,
            @AuthenticationPrincipal Jwt jwt) {
        authorization.requireOperator(jwt, "notifications:operations:read");
        return map(deliveries.getDelivery(UUID.fromString(id)));
    }

    @QueryMapping
    public List<ProviderHealthView> notificationProviderHealth(@AuthenticationPrincipal Jwt jwt) {
        authorization.requireOperator(jwt, "notifications:operations:read");
        return deliveries.providerHealth().stream().map(this::map).toList();
    }

    @MutationMapping
    public DeliveryView retryNotificationDelivery(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var actor = authorization.requireOperator(jwt, "notifications:delivery:retry");
        var result = deliveries.repair(new NotificationDeliveryRetryService.RepairCommand(
            UUID.fromString(required(input, "deliveryId")), RepairAction.RETRY_NOW,
            integer(input, "expectedVersion"), required(input, "reason"), null,
            required(input, "idempotencyKey"), actor.accountType(), actor.accountId()));
        return map(result);
    }

    private static DeliveryView map(NotificationDeliveryRetryService.DeliveryView view) {
        return new DeliveryView(view.id(), view.notificationId(), view.channel(), apiStatus(view.status()),
            view.provider(), view.templateRevision(), view.policyRevision(), view.destinationMasked(),
            view.contactSnapshotRevision(), view.attemptCount(), view.maximumAttempts(),
            view.nextAttemptAt() == null ? null : view.nextAttemptAt().toString(), view.lastErrorCode(),
            view.lastErrorClass(), view.providerMessageId(), view.revision(),
            view.updatedAt() == null ? null : view.updatedAt().toString());
    }

    private ProviderHealthView map(NotificationDeliveryRetryService.ProviderHealthView view) {
        return new ProviderHealthView(view.provider(), view.status(), view.circuitState(),
            view.failureRate(), view.latencyMilliseconds(), view.retryAfterSeconds(),
            view.checkedAt() == null ? null : view.checkedAt().toString());
    }

    private static String apiStatus(String status) {
        return switch (status) {
            case "PENDING" -> "QUEUED";
            case "FAILED" -> "FAILED";
            case "SENDING" -> "PROCESSING";
            default -> status;
        };
    }

    private static String required(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return String.valueOf(value);
    }

    private static int integer(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(required(input, field));
    }

    public record DeliveryView(
            UUID id,
            UUID notificationId,
            String channel,
            String status,
            String provider,
            Integer templateRevision,
            Integer policyRevision,
            String destinationMasked,
            Long contactSnapshotRevision,
            int attemptCount,
            int maximumAttempts,
            String nextAttemptAt,
            String lastErrorCode,
            String lastErrorClass,
            String providerMessageId,
            int revision,
            String updatedAt) {}

    public record ProviderHealthView(
            String provider,
            String status,
            String circuitState,
            double failureRate,
            int latencyMilliseconds,
            Integer retryAfterSeconds,
            String checkedAt) {}
}
