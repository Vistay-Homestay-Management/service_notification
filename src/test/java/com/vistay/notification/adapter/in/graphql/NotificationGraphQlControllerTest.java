package com.vistay.notification.adapter.in.graphql;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.oauth2.jwt.Jwt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationGraphQlControllerTest {
    @Mock
    private JdbcClient jdbc;

    @Test
    @SuppressWarnings("unchecked")
    void resumesAfterTheDurableCursorWithinTheAuthenticatedAccountBoundary() {
        var accountId = UUID.randomUUID();
        var notificationId = UUID.randomUUID();
        var jwt = mock(Jwt.class);
        var statement = mock(JdbcClient.StatementSpec.class);
        var query = mock(JdbcClient.MappedQuerySpec.class);

        when(jwt.getSubject()).thenReturn(accountId.toString());
        when(jdbc.sql(contains("event_sequence > :cursor"))).thenReturn(statement);
        when(statement.param(anyString(), any())).thenReturn(statement);
        when(statement.query(NotificationGraphQlController.NotificationRow.class)).thenReturn(query);
        when(query.list()).thenReturn(List.of(new NotificationGraphQlController.NotificationRow(
            42L,
            notificationId,
            "BACKOFFICE_WELCOME",
            "Chào mừng",
            "Tài khoản đã sẵn sàng.",
            OffsetDateTime.parse("2026-10-26T08:00:00+07:00"),
            null
        )));

        var event = new NotificationGraphQlController(jdbc)
            .notificationStream("41", jwt)
            .blockFirst(Duration.ofSeconds(1));

        assertEquals("42", event.cursor());
        assertEquals(notificationId, event.notification().id());
        assertEquals("WELCOME", event.notification().category());
        assertTrue(event.notification().channels().contains("IN_APP"));
        verify(statement).param("accountId", accountId);
        verify(statement).param("cursor", 41L);
    }

    @Test
    void rejectsAReplayCursorOutsideTheCanonicalNonNegativeFormat() {
        var jwt = mock(Jwt.class);
        when(jwt.getSubject()).thenReturn(UUID.randomUUID().toString());

        assertThrows(IllegalArgumentException.class,
            () -> new NotificationGraphQlController(jdbc).notificationStream("-1", jwt));
        assertThrows(IllegalArgumentException.class,
            () -> new NotificationGraphQlController(jdbc).notificationStream("01", jwt));
    }
}
