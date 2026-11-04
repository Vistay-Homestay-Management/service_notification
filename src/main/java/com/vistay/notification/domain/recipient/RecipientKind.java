package com.vistay.notification.domain.recipient;

/** Stable recipient rule kinds used in resolution evidence and GraphQL. */
public enum RecipientKind {
    DIRECT_ACCOUNT,
    ORGANIZATION_ROLE,
    BRANCH_ROLE,
    BOOKING_PARTY,
    EXPLICIT_SUBSCRIPTION
}
