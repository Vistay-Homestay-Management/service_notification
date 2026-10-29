package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import com.vistay.notification.application.NotificationAuthorizationClient.AuthorizationException;
import com.vistay.notification.application.NotificationAuthorizationClient.Scope;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/** Enforces recipient-level data scope for Notification APIs. */
@Service
public class NotificationAuthorizationService {
    private static final String READ_PERMISSION = "notification:read";

    private final NotificationAuthorizationClient authorizations;

    public NotificationAuthorizationService(NotificationAuthorizationClient authorizations) {
        this.authorizations = authorizations;
    }

    public AuthorizedRecipient requireOwnNotifications(Jwt jwt) {
        Principal principal = principal(jwt);
        try {
            var snapshot = authorizations.require(
                    principal.accountType(), principal.accountId(), READ_PERMISSION);
            var scopes = snapshot.scopes(READ_PERMISSION);
            if (!scopes.contains(Scope.SELF) && !scopes.contains(Scope.ALL)) {
                throw forbidden("notification:read requires SELF or ALL scope");
            }
            return new AuthorizedRecipient(principal.accountType().name(), principal.accountId());
        } catch (AuthorizationException denied) {
            HttpStatus status = denied.code().startsWith("AUTHORIZATION_")
                    ? HttpStatus.SERVICE_UNAVAILABLE
                    : HttpStatus.FORBIDDEN;
            throw new ResponseStatusException(status, denied.getMessage());
        }
    }

    private static Principal principal(Jwt jwt) {
        if (jwt == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                    "Missing authenticated principal");
        }
        UUID accountId;
        try {
            accountId = UUID.fromString(jwt.getSubject());
        } catch (IllegalArgumentException | NullPointerException invalid) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                    "JWT subject is not a ViStay account id");
        }
        String rawType = jwt.getClaimAsString("account_type");
        try {
            return new Principal(AccountKind.valueOf(
                    rawType == null ? "" : rawType.trim().toUpperCase()), accountId);
        } catch (IllegalArgumentException invalid) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                    "JWT account_type must be CUSTOMER, ADMIN or HOST");
        }
    }

    private static ResponseStatusException forbidden(String reason) {
        return new ResponseStatusException(HttpStatus.FORBIDDEN, "Access denied: " + reason);
    }

    private record Principal(AccountKind accountType, UUID accountId) {}

    public record AuthorizedRecipient(String accountType, UUID accountId) {}
}
