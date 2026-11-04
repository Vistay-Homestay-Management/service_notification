package com.vistay.notification.domain.recipient;

public enum RecipientExceptionType {
    NO_RECIPIENT,
    AMBIGUOUS_RECIPIENT,
    CROSS_PARTITION,
    MISSING_CONTACT,
    INVALID_CONTACT,
    UNVERIFIED_CONTACT,
    OPTIONAL_SUPPRESSED,
    DEPENDENCY_UNAVAILABLE
}
