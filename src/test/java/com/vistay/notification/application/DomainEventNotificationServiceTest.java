package com.vistay.notification.application;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DomainEventNotificationServiceTest {
    @Mock
    private JdbcClient jdbc;

    private final ObjectMapper json = JsonMapper.builder().build();

    @Test
    void quarantinesMalformedEnvelopeWithoutCreatingAInboxRow() {
        var quarantine = mock(JdbcClient.StatementSpec.class);
        when(jdbc.sql(org.mockito.ArgumentMatchers.startsWith(
            "INSERT INTO notification_intake_quarantine"))).thenReturn(quarantine);
        when(quarantine.param(anyString(), nullable(Object.class))).thenReturn(quarantine);
        when(quarantine.update()).thenReturn(1);

        var result = service().ingest("{\"eventId\":\"not-a-uuid\"}");

        assertEquals(DomainEventNotificationService.Outcome.QUARANTINED, result.outcome());
        assertEquals("MALFORMED_ENVELOPE", result.code());
    }

    @Test
    void quarantinesAnUnknownRouteBeforeRecipientResolution() {
        var quarantine = mock(JdbcClient.StatementSpec.class);
        when(jdbc.sql(org.mockito.ArgumentMatchers.startsWith(
            "INSERT INTO notification_intake_quarantine"))).thenReturn(quarantine);
        when(quarantine.param(anyString(), nullable(Object.class))).thenReturn(quarantine);
        when(quarantine.update()).thenReturn(1);

        var event = """
            {
              "eventId":"%s",
              "type":"vistay.unknown.event",
              "version":1,
              "aggregateId":"%s",
              "aggregateVersion":1,
              "correlationId":"%s",
              "causationId":null,
              "occurredAt":"2026-11-04T03:00:00Z",
              "data":{"recipientAccountId":"%s","recipientAccountType":"ADMIN"}
            }
            """.formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        var result = service().ingest(event);

        assertEquals(DomainEventNotificationService.Outcome.QUARANTINED, result.outcome());
        assertEquals("UNSUPPORTED_EVENT_ROUTE", result.code());
    }

    @Test
    void fingerprintsAreDeterministicAndDoNotDependOnJvmIdentity() {
        assertEquals(
            DomainEventNotificationService.fingerprint("same-event"),
            DomainEventNotificationService.fingerprint("same-event"));
    }

    private DomainEventNotificationService service() {
        return new DomainEventNotificationService(
            jdbc, json, new NotificationEventRouteRegistry(), mock(NotificationRecipientResolver.class));
    }
}
