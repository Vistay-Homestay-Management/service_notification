package com.vistay.notification.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class NotificationInboxStreamTest {
    @Test
    void acceptsOnlyCanonicalNonNegativeCursors() {
        assertEquals(0L, NotificationInboxService.parseCursor(null));
        assertEquals(42L, NotificationInboxService.parseCursor("42"));
        assertThrows(NotificationCommandException.class,
            () -> NotificationInboxService.parseCursor("01"));
        assertThrows(NotificationCommandException.class,
            () -> NotificationInboxService.parseCursor("-1"));
        assertThrows(NotificationCommandException.class,
            () -> NotificationInboxService.parseCursor("999999999999999999999999"));
    }
}
