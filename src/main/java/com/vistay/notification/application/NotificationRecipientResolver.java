package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import com.vistay.notification.application.NotificationAuthorizationClient.AuthorizationException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/** Resolves a recipient through the User authorization boundary only. */
@Component
public class NotificationRecipientResolver {
    private static final String RESOLVE_PERMISSION = "notification:recipient:resolve";

    private final NotificationAuthorizationClient authorizations;

    public NotificationRecipientResolver(NotificationAuthorizationClient authorizations) {
        this.authorizations = authorizations;
    }

    public ResolvedRecipient resolve(
            JsonNode data,
            NotificationEventRouteRegistry.EventRoute route) {
        if (data == null || !data.isObject()) {
            throw new RecipientResolutionException("NO_RECIPIENT", "Event data is not an object");
        }

        String explicitRecipientId = firstText(data, "recipientAccountId");
        String fallbackRecipientId = firstText(data, "recipientUserId");
        String accountIdValue = firstText(data, "accountId");
        if (explicitRecipientId != null && fallbackRecipientId != null
                && !explicitRecipientId.equals(fallbackRecipientId)) {
            throw new RecipientResolutionException(
                "AMBIGUOUS_RECIPIENT", "Event contains conflicting recipient references");
        }
        if (explicitRecipientId != null && accountIdValue != null
                && !explicitRecipientId.equals(accountIdValue)) {
            throw new RecipientResolutionException(
                "AMBIGUOUS_RECIPIENT", "Event contains conflicting account references");
        }
        String rawAccountId = explicitRecipientId != null
            ? explicitRecipientId : (fallbackRecipientId != null ? fallbackRecipientId : accountIdValue);
        if (rawAccountId == null) {
            throw new RecipientResolutionException("NO_RECIPIENT", "Event has no recipient reference");
        }
        UUID accountId;
        try {
            accountId = UUID.fromString(rawAccountId);
        } catch (IllegalArgumentException invalid) {
            throw new RecipientResolutionException("INVALID_RECIPIENT", "Recipient reference is invalid");
        }

        String explicitType = firstText(data, "recipientAccountType");
        String fallbackType = firstText(data, "accountType");
        String realmType = firstText(data, "realm");
        AccountKind accountType = accountType(explicitType != null
            ? explicitType : (fallbackType != null ? fallbackType : realmType));
        if (explicitType != null && fallbackType != null
                && !accountType(explicitType).equals(accountType(fallbackType))) {
            throw new RecipientResolutionException(
                "CROSS_PARTITION", "Recipient account types disagree");
        }
        if (explicitType != null && realmType != null
                && !accountType(explicitType).equals(accountType(realmType))) {
            throw new RecipientResolutionException(
                "CROSS_PARTITION", "Recipient realm disagrees with account type");
        }
        try {
            var snapshot = authorizations.resolveRecipient(accountType, accountId);
            String deviceReference = firstText(data, "deviceReference");
            if (deviceReference != null && !deviceReference.matches("[A-Za-z0-9._:-]{1,180}")) {
                throw new RecipientResolutionException("INVALID_CONTACT", "Device reference is invalid");
            }
            Long contactSnapshotRevision = positiveOptionalLong(data, "contactSnapshotRevision");
            String fingerprint = fingerprint(accountType.name(), accountId.toString(),
                snapshot.profileId().toString(), Long.toString(snapshot.authorizationVersion()),
                route.routeKey(), deviceReference,
                contactSnapshotRevision == null ? null : contactSnapshotRevision.toString());
            return new ResolvedRecipient(
                accountType,
                accountId,
                snapshot.profileId(),
                snapshot.authorizationVersion(),
                snapshot.authorizationVersion(),
                contactSnapshotRevision,
                deviceReference,
                fingerprint,
                RESOLVE_PERMISSION
            );
        } catch (AuthorizationException unavailable) {
            throw new RecipientResolutionException(unavailable.code(), unavailable.getMessage());
        }
    }

    private static AccountKind accountType(String rawType) {
        if (rawType == null || rawType.isBlank()) {
            throw new RecipientResolutionException("NO_RECIPIENT_TYPE", "Recipient account type is missing");
        }
        String normalized = rawType.trim().toUpperCase();
        if ("BACKOFFICE".equals(normalized)) {
            normalized = "ADMIN";
        }
        try {
            return AccountKind.valueOf(normalized);
        } catch (IllegalArgumentException invalid) {
            throw new RecipientResolutionException("CROSS_PARTITION", "Recipient account type is unsupported");
        }
    }

    private static String firstText(JsonNode data, String... fields) {
        for (String field : fields) {
            JsonNode value = data.get(field);
            if (value != null && value.isTextual() && !value.asText().isBlank()) {
                return value.asText();
            }
        }
        return null;
    }

    private static Long positiveOptionalLong(JsonNode data, String field) {
        JsonNode value = data.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isIntegralNumber() || value.asLong() < 1) {
            throw new RecipientResolutionException("INVALID_CONTACT", field + " must be positive");
        }
        return value.asLong();
    }

    static String fingerprint(String... values) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                digest.update((value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    public record ResolvedRecipient(
            AccountKind accountType,
            UUID accountId,
            UUID profileId,
            long authorizationVersion,
            long resolutionRevision,
            Long contactSnapshotRevision,
            String deviceReference,
            String snapshotFingerprint,
            String resolutionPermission) {}

    public static final class RecipientResolutionException extends RuntimeException {
        private final String code;

        public RecipientResolutionException(String code, String message) {
            super(message);
            this.code = code;
        }

        public String code() {
            return code;
        }
    }
}
