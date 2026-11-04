package com.vistay.notification.domain.recipient;

public enum ResolutionState {
    PENDING,
    RESOLVED,
    PARTIALLY_RESOLVED,
    NO_RECIPIENT,
    INVALID_CONTACT,
    FAILED_DEPENDENCY
}
