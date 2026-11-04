package com.vistay.notification.application;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * Explicit allow-list for source events that may create notifications.
 *
 * <p>The registry deliberately does not match a namespace or a wildcard. A
 * source service must add a route and a version before its event can create a
 * durable inbox item.</p>
 */
@Component
public class NotificationEventRouteRegistry {
    private static final Set<String> COMMON_SAFE_FIELDS = Set.of(
        "recipientAccountId", "recipientAccountType", "recipientUserId", "accountId",
        "accountType", "realm", "profileId", "displayName", "referenceId", "bookingId",
        "propertyId", "paymentIntentId", "financePaymentRequestId", "inventoryBlockId",
        "publicationId", "taskId", "caseId", "incidentId", "status", "action",
        "matchStatus", "reasonCode", "message", "title", "effectiveFrom",
        "effectiveToExclusive", "occurredAt", "provisionedAt", "publishedAt", "expiresAt",
        "revokedAt", "reason", "sessionId", "email", "providerTransactionId",
        "referenceNumber", "currency", "amount", "providerOrderId", "profileVersion",
        "rateProfileVersionId", "reservationId", "guestAccountId", "hostAccountId",
        "organizationId", "branchId", "siteId", "role", "taskType", "severity",
        "securityNotice",
        "publicationRevision", "inventoryRevision", "blockVersion", "snapshotSchemaVersion",
        "profileFingerprint", "previousPublicationId", "commandId", "startDate",
        "endDateExclusive", "blockType", "mismatchReason", "notificationId",
        "deviceReference", "contactSnapshotRevision", "recipientKind"
    );

    private static final Set<String> SENSITIVE_FIELDS = Set.of(
        "password", "passwordHash", "accessToken", "refreshToken", "clientSecret",
        "providerPayload", "rawProviderPayload", "providerResponse", "rawPayload",
        "destination", "destinationAddress", "phoneNumber", "contactValue",
        "encryptedContact", "deviceToken"
    );

    private final Map<RouteKey, EventRoute> routes;

    public NotificationEventRouteRegistry() {
        var definitions = List.of(
            route("vistay.auth.account-provisioned", Domain.AUTH, "SECURITY",
                "security.account-provisioned", "auth.account-provisioned", "HIGH",
                "Account provisioned", "A new ViStay account is ready", true),
            route("vistay.auth.session-revoked", Domain.AUTH, "SECURITY",
                "security.session-revoked", "auth.session-revoked", "HIGH",
                "Session revoked", "A security session was revoked", false),

            route("vistay.booking.reservation-created", Domain.BOOKING, "BOOKING",
                "booking.reservation-created", "backoffice.reservation.created", "NORMAL",
                "Reservation created", "A reservation was created", true),
            route("vistay.booking.reservation-confirmed", Domain.BOOKING, "BOOKING",
                "booking.reservation-confirmed", "backoffice.reservation.confirmed", "HIGH",
                "Reservation confirmed", "A reservation was confirmed", true),
            route("vistay.booking.reservation-cancelled", Domain.BOOKING, "BOOKING",
                "booking.reservation-cancelled", "backoffice.reservation.cancelled", "HIGH",
                "Reservation cancelled", "A reservation was cancelled", true),
            route("vistay.booking.inventory-block-changed", Domain.BOOKING, "BOOKING",
                "booking.inventory-block-changed", "backoffice.inventory.block-changed", "NORMAL",
                "Inventory block changed", "Property inventory availability changed", false),
            route("vistay.booking.rate-profile-published", Domain.BOOKING, "BOOKING",
                "booking.rate-profile-published", "backoffice.rate-profile.published", "NORMAL",
                "Rate profile published", "A rate profile was published", false),

            route("vistay.operations.task-assigned", Domain.OPERATIONS, "OPERATIONS",
                "operations.task-assigned", "operations.task.assigned", "HIGH",
                "Task assigned", "An operations task was assigned", true),
            route("vistay.operations.task-escalated", Domain.OPERATIONS, "OPERATIONS",
                "operations.task-escalated", "operations.task.escalated", "URGENT",
                "Task escalated", "An operations task requires attention", true),
            route("vistay.operations.incident-raised", Domain.OPERATIONS, "OPERATIONS",
                "operations.incident-raised", "operations.incident.raised", "URGENT",
                "Incident raised", "An operations incident was raised", true),
            route("vistay.operations.maintenance-scheduled", Domain.OPERATIONS, "OPERATIONS",
                "operations.maintenance-scheduled", "operations.maintenance.scheduled", "NORMAL",
                "Maintenance scheduled", "Maintenance was scheduled", false),

            route("vistay.finance.payment-request-created", Domain.FINANCE, "FINANCE",
                "finance.payment-request-created", "finance.payment-request.created", "NORMAL",
                "Payment request created", "A finance payment request was created", true),
            route("vistay.finance.payment-request-approved", Domain.FINANCE, "FINANCE",
                "finance.payment-request-approved", "finance.payment-request.approved", "HIGH",
                "Payment request approved", "A finance payment request was approved", true),
            route("vistay.finance.payment-request-rejected", Domain.FINANCE, "FINANCE",
                "finance.payment-request-rejected", "finance.payment-request.rejected", "HIGH",
                "Payment request rejected", "A finance payment request was rejected", true),
            route("vistay.finance.payment-request-expired", Domain.FINANCE, "FINANCE",
                "finance.payment-request-expired", "finance.payment-request.expired", "HIGH",
                "Payment request expired", "A finance payment request expired", true),
            route("vistay.finance.payment-allocation-changed", Domain.FINANCE, "FINANCE",
                "finance.payment-allocation-changed", "finance.payment-allocation.changed", "NORMAL",
                "Payment allocation changed", "A finance payment allocation changed", false),

            route("vistay.payment.intent-created", Domain.FINANCE, "FINANCE",
                "finance.payment-intent-created", "finance.payment-intent.created", "NORMAL",
                "Payment intent created", "A payment intent was created", true),
            route("vistay.payment.intent-expired", Domain.FINANCE, "FINANCE",
                "finance.payment-intent-expired", "finance.payment-intent.expired", "HIGH",
                "Payment intent expired", "A payment intent expired", true),
            route("vistay.payment.qr-ready", Domain.FINANCE, "FINANCE",
                "finance.payment-qr-ready", "finance.payment.qr-ready", "NORMAL",
                "Payment QR ready", "A payment QR code is ready", true),
            route("vistay.payment.transaction-confirmed", Domain.FINANCE, "FINANCE",
                "finance.transaction-confirmed", "finance.transaction.confirmed", "HIGH",
                "Payment confirmed", "A payment transaction was confirmed", true),
            route("vistay.payment.transaction-unmatched", Domain.FINANCE, "FINANCE",
                "finance.transaction-unmatched", "finance.transaction.unmatched", "URGENT",
                "Payment unmatched", "A payment transaction needs reconciliation", true),

            route("vistay.system.announcement-published", Domain.SYSTEM, "SYSTEM",
                "system.announcement-published", "system.announcement.published", "NORMAL",
                "Announcement", "A new system announcement is available", true),
            route("vistay.system.maintenance-started", Domain.SYSTEM, "SYSTEM",
                "system.maintenance-started", "system.maintenance.started", "HIGH",
                "Maintenance started", "Planned system maintenance started", true),
            route("vistay.system.service-degraded", Domain.SYSTEM, "SYSTEM",
                "system.service-degraded", "system.service.degraded", "URGENT",
                "Service degraded", "A ViStay service is degraded", true)
        );
        routes = definitions.stream().collect(Collectors.toUnmodifiableMap(
            route -> new RouteKey(route.eventType(), route.eventVersion()), Function.identity()));
    }

    public EventRoute find(String eventType, int eventVersion) {
        return routes.get(new RouteKey(eventType, eventVersion));
    }

    public List<EventRoute> routes() {
        return List.copyOf(routes.values());
    }

    public static Set<String> commonSafeFields() {
        return COMMON_SAFE_FIELDS;
    }

    public static Set<String> sensitiveFields() {
        return SENSITIVE_FIELDS;
    }

    private static EventRoute route(
            String eventType,
            Domain domain,
            String category,
            String notificationType,
            String templateKey,
            String priority,
            String title,
            String body,
            boolean pushAllowed) {
        return new EventRoute(
            eventType, 1, eventType.substring("vistay.".length()), 1, domain,
            category, notificationType, priority, templateKey, title, body,
            List.of("IN_APP", "EMAIL"), pushAllowed ? List.of("PUSH") : List.of(),
            Set.copyOf(COMMON_SAFE_FIELDS));
    }

    public enum Domain { AUTH, BOOKING, OPERATIONS, FINANCE, SYSTEM }

    public record EventRoute(
            String eventType,
            int eventVersion,
            String routeKey,
            int routeVersion,
            Domain domain,
            String category,
            String notificationType,
            String priority,
            String templateKey,
            String defaultTitle,
            String defaultBody,
            List<String> requiredChannels,
            List<String> optionalChannels,
            Set<String> safeFields) {}

    private record RouteKey(String eventType, int eventVersion) {}
}
