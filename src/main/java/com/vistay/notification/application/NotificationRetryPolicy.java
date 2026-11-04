package com.vistay.notification.application;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

/** Versioned, bounded retry policy shared by the scheduler and repair API. */
public final class NotificationRetryPolicy {
    private NotificationRetryPolicy() {}

    public static ErrorClass classify(String errorCode, String errorClass) {
        if (errorClass != null) {
            try {
                return ErrorClass.valueOf(errorClass.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                // Fall through to the stable code mapping.
            }
        }
        String code = errorCode == null ? "" : errorCode.toUpperCase(Locale.ROOT);
        if (code.contains("INVALID") || code.contains("MISSING_CONTACT")
                || code.contains("UNAUTHORIZED") || code.contains("PERMANENT")) {
            return ErrorClass.PERMANENT;
        }
        if (code.contains("RATE") || code.contains("THROTTLE")) {
            return ErrorClass.RATE_LIMITED;
        }
        if (code.contains("BOUNCE")) {
            return ErrorClass.BOUNCED;
        }
        if (code.contains("PROVIDER") || code.contains("TIMEOUT") || code.contains("UNAVAILABLE")) {
            return ErrorClass.PROVIDER_OUTAGE;
        }
        return ErrorClass.TRANSIENT;
    }

    public static boolean retryable(ErrorClass errorClass) {
        return errorClass == ErrorClass.TRANSIENT
                || errorClass == ErrorClass.RATE_LIMITED
                || errorClass == ErrorClass.PROVIDER_OUTAGE;
    }

    public record Policy(
            String key,
            int revision,
            int maxAttempts,
            Duration maxAge,
            Duration initialDelay,
            Duration maximumDelay,
            int jitterPercent,
            int circuitFailureThreshold,
            Duration circuitOpenDuration,
            Duration latencyThreshold) {
        public Policy(
                String key,
                int revision,
                int maxAttempts,
                Duration maxAge,
                Duration initialDelay,
                Duration maximumDelay,
                int jitterPercent,
                int circuitFailureThreshold,
                Duration circuitOpenDuration) {
            this(key, revision, maxAttempts, maxAge, initialDelay, maximumDelay,
                jitterPercent, circuitFailureThreshold, circuitOpenDuration, Duration.ofSeconds(2));
        }

        public Policy {
            if (revision < 1 || maxAttempts < 1 || maxAge.isNegative() || maxAge.isZero()
                    || initialDelay.isNegative() || maximumDelay.isNegative()
                    || maximumDelay.compareTo(initialDelay) < 0
                    || jitterPercent < 0 || jitterPercent > 100
                    || circuitFailureThreshold < 1 || circuitOpenDuration.isNegative()
                    || circuitOpenDuration.isZero() || latencyThreshold.isNegative()
                    || latencyThreshold.isZero()) {
                throw new IllegalArgumentException("Invalid notification retry policy");
            }
        }

        public Duration backoff(int attemptNumber) {
            if (attemptNumber < 1) {
                throw new IllegalArgumentException("attempt number must be positive");
            }
            long seconds = initialDelay.toSeconds();
            int shifts = Math.min(attemptNumber - 1, 30);
            long multiplier = 1L << shifts;
            long candidate;
            try {
                candidate = Math.multiplyExact(seconds, multiplier);
            } catch (ArithmeticException overflow) {
                candidate = Long.MAX_VALUE;
            }
            return Duration.ofSeconds(Math.min(candidate, maximumDelay.toSeconds()));
        }

        public Duration backoffWithJitter(int attemptNumber, double jitterFactor) {
            if (jitterFactor < -1 || jitterFactor > 1) {
                throw new IllegalArgumentException("jitter factor must be between -1 and 1");
            }
            Duration base = backoff(attemptNumber);
            long seconds = Math.max(0, Math.round(base.toMillis() * (1 + jitterFactor)) / 1000);
            return Duration.ofSeconds(Math.min(seconds, maximumDelay.toSeconds()));
        }

        public Instant nextAttemptAt(Instant now, int nextAttemptNumber, Instant retryAfter) {
            if (retryAfter != null && retryAfter.isAfter(now)) {
                return retryAfter;
            }
            return now.plus(backoff(nextAttemptNumber));
        }

        public boolean exhausted(int attemptCount, Instant firstFailureAt, Instant now) {
            return attemptCount >= maxAttempts || !now.isBefore(firstFailureAt.plus(maxAge));
        }
    }

    public enum ErrorClass {
        TRANSIENT,
        PERMANENT,
        RATE_LIMITED,
        PROVIDER_OUTAGE,
        INVALID_CONTACT,
        BOUNCED,
        UNKNOWN
    }
}
