package com.vistay.notification.application;

import com.vistay.notification.application.NotificationAuthorizationClient.AccountKind;
import java.util.UUID;

/**
 * Outbound boundary for the User-owned contact snapshot API. Implementations
 * may return only an immutable, purpose-bound snapshot; they must not expose
 * canonical User data or provider credentials to Notification.
 */
public interface NotificationContactResolverPort {
    ContactSnapshot resolve(AccountKind accountType, UUID accountId, String purpose, String fieldGroup);

    record ContactSnapshot(
            long revision,
            String destinationMasked,
            byte[] destinationCiphertext,
            VerificationStatus verificationStatus,
            String sourceReference,
            String snapshotFingerprint) {
        public ContactSnapshot {
            destinationCiphertext = destinationCiphertext == null
                ? null : destinationCiphertext.clone();
        }

        @Override
        public byte[] destinationCiphertext() {
            return destinationCiphertext == null ? null : destinationCiphertext.clone();
        }
    }

    enum VerificationStatus {
        VERIFIED,
        UNVERIFIED,
        INVALID,
        MISSING
    }
}
