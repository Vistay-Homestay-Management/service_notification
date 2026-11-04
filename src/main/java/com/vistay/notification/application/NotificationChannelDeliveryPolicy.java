package com.vistay.notification.application;

import com.vistay.notification.application.NotificationEventRouteRegistry.EventRoute;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Applies the notification channel contract without reading another service's
 * database. Required channels are never disabled by a recipient preference;
 * optional push is created only when every feature/device gate is satisfied.
 */
public final class NotificationChannelDeliveryPolicy {
    public static final String IN_APP = "IN_APP";
    public static final String EMAIL = "EMAIL";
    public static final String PUSH = "PUSH";

    private NotificationChannelDeliveryPolicy() {}

    public static DeliveryPlan evaluate(
            EventRoute route,
            PolicySnapshot policy,
            PreferenceSnapshot preferences,
            DeviceSnapshot device,
            Instant now) {
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(preferences, "preferences");
        Objects.requireNonNull(now, "now");

        List<ChannelIntent> intents = new ArrayList<>();
        List<String> required = new ArrayList<>(route.requiredChannels());
        if (!policy.requiredChannels().isEmpty()) {
            required = union(required, policy.requiredChannels());
        }
        for (String channel : required) {
            // Required channels are intentionally not preference-gated.
            intents.add(new ChannelIntent(channel, true, null));
        }

        List<String> optional = union(route.optionalChannels(), policy.optionalChannels());
        if (optional.contains(PUSH)
                && policy.globalPushEnabled()
                && policy.routeAllowsPush()
                && device != null
                && device.valid()) {
            Preference preference = preferences.forNotificationType(route.notificationType(), PUSH);
            if (preference == null || preference.enabled()) {
                Instant deferredUntil = quietHoursEnd(preference, now);
                intents.add(new ChannelIntent(PUSH, false, deferredUntil));
            }
        }

        return new DeliveryPlan(
                List.copyOf(intents),
                policy.revision(),
                preferences.revision(),
                policy.globalPushEnabled(),
                device == null ? null : device.reference(),
                policy.snapshot());
    }

    private static List<String> union(List<String> first, List<String> second) {
        Map<String, Boolean> values = new LinkedHashMap<>();
        first.forEach(channel -> values.put(channel, true));
        second.forEach(channel -> values.put(channel, true));
        return List.copyOf(values.keySet());
    }

    private static Instant quietHoursEnd(Preference preference, Instant now) {
        if (preference == null || preference.quietHoursStart() == null
                || preference.quietHoursEnd() == null) {
            return null;
        }
        ZoneId zone;
        try {
            zone = ZoneId.of(preference.timeZoneId());
        } catch (Exception invalidZone) {
            // A bad optional preference must not suppress a required channel or
            // create an unbounded retry loop. Treat optional push as disabled.
            return now.plus(Duration.ofMinutes(1));
        }
        ZonedDateTime localNow = now.atZone(zone);
        LocalTime start = preference.quietHoursStart();
        LocalTime end = preference.quietHoursEnd();
        LocalTime current = localNow.toLocalTime();
        boolean overnight = end.isBefore(start);
        boolean quiet = overnight
                ? !current.isBefore(start) || current.isBefore(end)
                : !current.isBefore(start) && current.isBefore(end);
        if (!quiet) {
            return null;
        }
        ZonedDateTime endAt = localNow.with(end);
        if (!endAt.isAfter(localNow)) {
            endAt = endAt.plusDays(1);
        }
        return endAt.toInstant();
    }

    public record PolicySnapshot(
            String policyKey,
            int revision,
            List<String> requiredChannels,
            List<String> optionalChannels,
            boolean globalPushEnabled,
            boolean routeAllowsPush,
            Map<String, Object> snapshot) {
        public PolicySnapshot {
            if (revision < 1) {
                throw new IllegalArgumentException("policy revision must be positive");
            }
            requiredChannels = List.copyOf(requiredChannels == null ? List.of() : requiredChannels);
            optionalChannels = List.copyOf(optionalChannels == null ? List.of() : optionalChannels);
            snapshot = Map.copyOf(snapshot == null ? Map.of() : snapshot);
        }

        public static PolicySnapshot fromRoute(EventRoute route, boolean globalPushEnabled) {
            return new PolicySnapshot(
                    "notification.default",
                    1,
                    route.requiredChannels(),
                    route.optionalChannels(),
                    globalPushEnabled,
                    !route.optionalChannels().isEmpty(),
                    Map.of("policyKey", "notification.default", "revision", 1,
                        "pushEnabled", globalPushEnabled));
        }
    }

    public record PreferenceSnapshot(int revision, Map<String, Preference> values) {
        public PreferenceSnapshot {
            if (revision < 1) {
                throw new IllegalArgumentException("preference revision must be positive");
            }
            values = Map.copyOf(values == null ? Map.of() : values);
        }

        public Preference forChannel(String channel) {
            return values.get(channel);
        }

        public Preference forNotificationType(String notificationType, String channel) {
            Preference exact = values.get(notificationType + "|" + channel);
            if (exact != null) {
                return exact;
            }
            Preference wildcard = values.get("*|" + channel);
            return wildcard == null ? values.get(channel) : wildcard;
        }
    }

    public record Preference(
            String channel,
            boolean enabled,
            LocalTime quietHoursStart,
            LocalTime quietHoursEnd,
            String timeZoneId) {}

    public record DeviceSnapshot(String reference, boolean valid, String provider) {}

    public record ChannelIntent(String channel, boolean required, Instant deferredUntil) {}

    public record DeliveryPlan(
            List<ChannelIntent> intents,
            int policyRevision,
            int preferenceRevision,
            boolean pushFeatureEnabled,
            String deviceReference,
            Map<String, Object> policySnapshot) {}
}
