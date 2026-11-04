package com.vistay.notification.adapter.in.messaging;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProfileProvisionedListenerTest {
    @Mock
    private JdbcClient jdbc;

    private final ObjectMapper json = JsonMapper.builder().build();

    @Test
    void doesNotCreateASecondWelcomeNotificationForARedeliveredEvent() throws Exception {
        var eventId = UUID.randomUUID();
        var accountId = UUID.randomUUID();
        var profileId = UUID.randomUUID();
        var notificationId = UUID.randomUUID();

        var processed = mock(JdbcClient.StatementSpec.class);
        var notification = mock(JdbcClient.StatementSpec.class);
        var notificationQuery = mock(JdbcClient.MappedQuerySpec.class);
        var delivery = mock(JdbcClient.StatementSpec.class);
        var outbox = mock(JdbcClient.StatementSpec.class);

        when(jdbc.sql(org.mockito.ArgumentMatchers.startsWith("INSERT INTO processed_events")))
            .thenReturn(processed);
        when(processed.param(anyString(), any())).thenReturn(processed);
        when(processed.update()).thenReturn(1, 0);

        when(jdbc.sql(org.mockito.ArgumentMatchers.startsWith("INSERT INTO notifications")))
            .thenReturn(notification);
        when(notification.param(anyString(), any())).thenReturn(notification);
        when(notification.query(ProfileProvisionedListener.NotificationCreatedRow.class))
            .thenReturn(notificationQuery);
        when(notificationQuery.single()).thenReturn(
            new ProfileProvisionedListener.NotificationCreatedRow(
                notificationId, 1, OffsetDateTime.now()
            )
        );

        when(jdbc.sql(org.mockito.ArgumentMatchers.startsWith("INSERT INTO notification_deliveries")))
            .thenReturn(delivery);
        when(delivery.param(anyString(), any())).thenReturn(delivery);
        when(delivery.update()).thenReturn(1);

        when(jdbc.sql(org.mockito.ArgumentMatchers.startsWith("INSERT INTO outbox_events")))
            .thenReturn(outbox);
        when(outbox.param(anyString(), any())).thenReturn(outbox);
        when(outbox.update()).thenReturn(1);

        var envelope = """
            {
              "eventId":"%s",
              "type":"vistay.user.profile-provisioned",
              "version":1,
              "aggregateId":"%s",
              "aggregateVersion":1,
              "correlationId":"%s",
              "causationId":"%s",
              "occurredAt":"%s",
              "data":{
                "accountId":"%s",
                "profileId":"%s",
                "realm":"BACKOFFICE",
                "email":"admin@vistay.vn",
                "displayName":"ViStay Admin",
                "authorizationVersion":1,
                "provisionedAt":"%s"
              }
            }
            """.formatted(eventId, profileId, UUID.randomUUID(), UUID.randomUUID(),
                OffsetDateTime.now(), accountId, profileId, OffsetDateTime.now());

        var listener = new ProfileProvisionedListener(jdbc, json);
        listener.onProfileProvisioned(envelope);
        listener.onProfileProvisioned(envelope);

        verify(notification, times(1)).query(ProfileProvisionedListener.NotificationCreatedRow.class);
        verify(outbox, times(1)).update();
    }
}
