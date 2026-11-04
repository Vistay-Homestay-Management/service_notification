package com.vistay.notification.application;

import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DomainEventNotificationRouteRegistryTest {
    private final NotificationEventRouteRegistry registry = new NotificationEventRouteRegistry();

    @Test
    void registersExplicitRoutesAcrossEveryEpicDomain() {
        assertNotNull(registry.find("vistay.auth.account-provisioned", 1));
        assertNotNull(registry.find("vistay.booking.reservation-created", 1));
        assertNotNull(registry.find("vistay.operations.task-assigned", 1));
        assertNotNull(registry.find("vistay.finance.payment-request-created", 1));
        assertNotNull(registry.find("vistay.system.announcement-published", 1));
        assertNull(registry.find("vistay.booking.not-registered", 1));
        assertNull(registry.find("vistay.auth.account-provisioned", 2));
    }

    @Test
    void freezesRequiredChannelPolicyAndDoesNotAllowAnImplicitWildcard() {
        var route = registry.find("vistay.booking.reservation-created", 1);

        assertEquals(Set.of("IN_APP", "EMAIL"), Set.copyOf(route.requiredChannels()));
        assertTrue(route.optionalChannels().contains("PUSH"));
        assertEquals("BOOKING", route.category());
        assertEquals("booking.reservation-created", route.notificationType());
        assertEquals(1, route.routeVersion());
    }
}
