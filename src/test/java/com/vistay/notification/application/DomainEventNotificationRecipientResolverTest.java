package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import com.vistay.notification.application.NotificationAuthorizationClient.Snapshot;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DomainEventNotificationRecipientResolverTest {
    @Mock
    private NotificationAuthorizationClient authorizations;

    private final ObjectMapper json = JsonMapper.builder().build();

    @Test
    void mapsBackofficeRealmAndFreezesTheUserAuthorizationRevision() {
        var accountId = UUID.randomUUID();
        var profileId = UUID.randomUUID();
        when(authorizations.resolveRecipient(AccountKind.ADMIN, accountId))
            .thenReturn(new Snapshot(true, profileId, 7, Map.of()));

        var route = new NotificationEventRouteRegistry()
            .find("vistay.auth.account-provisioned", 1);
        var recipient = new NotificationRecipientResolver(authorizations)
            .resolve(json.readTree("""
                {"accountId":"%s","realm":"BACKOFFICE"}
                """.formatted(accountId)), route);

        assertEquals(AccountKind.ADMIN, recipient.accountType());
        assertEquals(accountId, recipient.accountId());
        assertEquals(profileId, recipient.profileId());
        assertEquals(7, recipient.resolutionRevision());
        verify(authorizations).resolveRecipient(AccountKind.ADMIN, accountId);
    }

    @Test
    void rejectsCrossPartitionRecipientReferencesBeforeCallingUserService() {
        var accountId = UUID.randomUUID();
        var route = new NotificationEventRouteRegistry()
            .find("vistay.system.announcement-published", 1);

        var failure = assertThrows(
            NotificationRecipientResolver.RecipientResolutionException.class,
            () -> new NotificationRecipientResolver(authorizations).resolve(
                json.readTree("""
                    {"recipientAccountId":"%s","recipientAccountType":"CUSTOMER","realm":"BACKOFFICE"}
                    """.formatted(accountId)), route));

        assertEquals("CROSS_PARTITION", failure.code());
    }
}
