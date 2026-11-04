package com.vistay.notification.application;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

/** Outbound provider boundary; implementations must never receive raw contact values. */
public interface NotificationProviderPort {
    String channel();

    String provider();

    SendResult send(SendRequest request);

    record SendRequest(
            UUID deliveryId,
            String channel,
            String providerIdempotencyKey,
            String destinationMasked,
            byte[] encryptedDestinationSnapshot,
            String subject,
            String body,
            Duration timeout) {}

    record SendResult(
            Outcome outcome,
            String providerMessageId,
            String errorCode,
            String errorClass,
            OffsetDateTime retryAt) {
        public static SendResult accepted(String providerMessageId) {
            return new SendResult(Outcome.ACCEPTED, providerMessageId, null, null, null);
        }

        public static SendResult failed(String code, String errorClass, OffsetDateTime retryAt) {
            return new SendResult(Outcome.FAILED, null, code, errorClass, retryAt);
        }
    }

    enum Outcome { ACCEPTED, FAILED }
}
