package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import com.vistay.notification.application.NotificationAuthorizationClient.AuthorizationException;
import com.vistay.notification.application.NotificationAuthorizationClient.Scope;
import com.vistay.notification.application.NotificationAuthorizationClient.Snapshot;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationAuthorizationServiceTest {
    @Mock
    private NotificationAuthorizationClient client;

    @Test
    void authorizesTheJwtAccountAsItsOwnNotificationRecipient() {
        var accountId = UUID.randomUUID();
        var jwt = jwt(accountId, "host");
        when(client.require(AccountKind.HOST, accountId, "notification:read"))
            .thenReturn(snapshot(Scope.SELF));

        var recipient = new NotificationAuthorizationService(client).requireOwnNotifications(jwt);

        assertEquals("HOST", recipient.accountType());
        assertEquals(accountId, recipient.accountId());
        verify(client).require(AccountKind.HOST, accountId, "notification:read");
    }

    @Test
    void allowsAllButRejectsUnrelatedRecordScopes() {
        var accountId = UUID.randomUUID();
        var jwt = jwt(accountId, "ADMIN");
        when(client.require(AccountKind.ADMIN, accountId, "notification:read"))
            .thenReturn(snapshot(Scope.ALL))
            .thenReturn(snapshot(Scope.ASSIGNED));

        var service = new NotificationAuthorizationService(client);
        assertEquals(accountId, service.requireOwnNotifications(jwt).accountId());

        var denied = assertThrows(ResponseStatusException.class,
            () -> service.requireOwnNotifications(jwt));
        assertEquals(HttpStatus.FORBIDDEN, denied.getStatusCode());
    }

    @Test
    void mapsAnUnavailableAuthorizationSnapshotToServiceUnavailable() {
        var accountId = UUID.randomUUID();
        var jwt = jwt(accountId, "CUSTOMER");
        when(client.require(AccountKind.CUSTOMER, accountId, "notification:read"))
            .thenThrow(new AuthorizationException(
                "AUTHORIZATION_UNAVAILABLE", "Authorization service is unavailable"));

        var failure = assertThrows(ResponseStatusException.class,
            () -> new NotificationAuthorizationService(client).requireOwnNotifications(jwt));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, failure.getStatusCode());
    }

    @Test
    void rejectsInvalidTokenIdentityBeforeCallingUserService() {
        var jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn("not-a-uuid");

        var failure = assertThrows(ResponseStatusException.class,
            () -> new NotificationAuthorizationService(client).requireOwnNotifications(jwt));

        assertEquals(HttpStatus.UNAUTHORIZED, failure.getStatusCode());
    }

    private static Jwt jwt(UUID accountId, String accountType) {
        var jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn(accountId.toString());
        when(jwt.getClaimAsString("account_type")).thenReturn(accountType);
        return jwt;
    }

    private static Snapshot snapshot(Scope scope) {
        return new Snapshot(true, UUID.randomUUID(), 1,
            Map.of("notification:read", Set.of(scope)));
    }
}
