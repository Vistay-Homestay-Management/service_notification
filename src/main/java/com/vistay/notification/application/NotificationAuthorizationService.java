package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import com.vistay.notification.application.NotificationAuthorizationClient.AuthorizationException;
import com.vistay.notification.application.NotificationAuthorizationClient.Scope;
import java.util.Set;
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

    public AuthorizedRecipient requireOwnPreferences(Jwt jwt, boolean manage) {
        Principal principal = principal(jwt);
        String permission = manage ? "notifications:preferences:manage" : "notifications:read";
        try {
            var snapshot = authorizations.require(principal.accountType(), principal.accountId(), permission);
            Set<Scope> scopes = snapshot.scopes(permission);
            if (!scopes.contains(Scope.SELF) && !scopes.contains(Scope.ALL)) {
                throw forbidden(permission + " requires SELF or ALL scope");
            }
            return new AuthorizedRecipient(principal.accountType().name(), principal.accountId());
        } catch (AuthorizationException denied) {
            throw toResponseStatus(denied);
        }
    }

    public AuthorizedActor requireOperator(Jwt jwt, String permission) {
        Principal principal = principal(jwt);
        try {
            var snapshot = authorizations.require(principal.accountType(), principal.accountId(), permission);
            Set<Scope> scopes = snapshot.scopes(permission);
            if (!scopes.contains(Scope.PARTITION) && !scopes.contains(Scope.ALL)) {
                throw forbidden(permission + " requires PARTITION or ALL scope");
            }
            return new AuthorizedActor(principal.accountType().name(), principal.accountId());
        } catch (AuthorizationException denied) {
            throw toResponseStatus(denied);
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

    private static ResponseStatusException toResponseStatus(AuthorizationException denied) {
        HttpStatus status = denied.code().startsWith("AUTHORIZATION_")
                ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.FORBIDDEN;
        return new ResponseStatusException(status, denied.getMessage());
    }

    private record Principal(AccountKind accountType, UUID accountId) {}

    public record AuthorizedRecipient(String accountType, UUID accountId) {}

    public record AuthorizedActor(String accountType, UUID accountId) {}
}
