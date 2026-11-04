package com.vistay.notification.application;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NotificationChannelDeliveryPolicyTest {
    private final NotificationEventRouteRegistry routes = new NotificationEventRouteRegistry();

    @Test
    void requiredInAppAndEmailRemainEnabledWhenPreferenceDisablesEmail() {
        var route = routes.find("vistay.booking.reservation-confirmed", 1);
        var policy = NotificationChannelDeliveryPolicy.PolicySnapshot.fromRoute(route, false);
        var preferences = new NotificationChannelDeliveryPolicy.PreferenceSnapshot(7, Map.of(
            "EMAIL", new NotificationChannelDeliveryPolicy.Preference(
                "EMAIL", false, null, null, "Asia/Ho_Chi_Minh")));

        var plan = NotificationChannelDeliveryPolicy.evaluate(
            route, policy, preferences, null, Instant.parse("2026-11-04T03:00:00Z"));

        assertEquals(2, plan.intents().size());
        assertTrue(plan.intents().stream().allMatch(NotificationChannelDeliveryPolicy.ChannelIntent::required));
    }

    @Test
    void pushRequiresGlobalFlagRoutePermissionDeviceAndQuietHoursAreBounded() {
        var route = routes.find("vistay.booking.reservation-created", 1);
        var policy = new NotificationChannelDeliveryPolicy.PolicySnapshot(
            "notification.default", 3, route.requiredChannels(), route.optionalChannels(),
            true, true, Map.of("pushEnabled", true));
        var preferences = new NotificationChannelDeliveryPolicy.PreferenceSnapshot(4, Map.of(
            "PUSH", new NotificationChannelDeliveryPolicy.Preference(
                "PUSH", true, LocalTime.of(20, 0), LocalTime.of(8, 0), "Asia/Ho_Chi_Minh")));
        var device = new NotificationChannelDeliveryPolicy.DeviceSnapshot("device-ref-1", true, "fcm");

        var plan = NotificationChannelDeliveryPolicy.evaluate(
            route, policy, preferences, device, Instant.parse("2026-11-04T15:00:00Z"));

        var push = plan.intents().stream()
            .filter(intent -> "PUSH".equals(intent.channel()))
            .findFirst()
            .orElseThrow();
        assertTrue(push.deferredUntil() != null);
        assertEquals(3, plan.policyRevision());
        assertEquals(4, plan.preferenceRevision());
    }
}
