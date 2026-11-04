package com.vistay.notification.application;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NotificationDeliveryRetryPolicyTest {
    private final NotificationRetryPolicy.Policy policy = new NotificationRetryPolicy.Policy(
        "notification.default", 1, 5, Duration.ofHours(1), Duration.ofSeconds(30),
        Duration.ofMinutes(5), 20, 3, Duration.ofSeconds(30));

    @Test
    void classifiesProviderFailuresAndCapsExponentialBackoff() {
        assertEquals(NotificationRetryPolicy.ErrorClass.PROVIDER_OUTAGE,
            NotificationRetryPolicy.classify("PROVIDER_UNAVAILABLE", null));
        assertEquals(NotificationRetryPolicy.ErrorClass.RATE_LIMITED,
            NotificationRetryPolicy.classify("HTTP_429_RATE_LIMIT", null));
        assertEquals(Duration.ofSeconds(30), policy.backoff(1));
        assertEquals(Duration.ofSeconds(60), policy.backoff(2));
        assertEquals(Duration.ofMinutes(5), policy.backoff(20));
        assertTrue(NotificationRetryPolicy.retryable(NotificationRetryPolicy.ErrorClass.TRANSIENT));
        assertFalse(NotificationRetryPolicy.retryable(NotificationRetryPolicy.ErrorClass.BOUNCED));
    }

    @Test
    void retryAfterWinsButAttemptAndAgeStillTerminate() {
        Instant now = Instant.parse("2026-11-04T03:00:00Z");
        Instant retryAfter = now.plusSeconds(7);
        assertEquals(retryAfter, policy.nextAttemptAt(now, 1, retryAfter));
        assertFalse(policy.exhausted(4, now, now.plusSeconds(30)));
        assertTrue(policy.exhausted(5, now, now.plusSeconds(30)));
        assertTrue(policy.exhausted(1, now, now.plus(Duration.ofHours(1))));
    }
}
