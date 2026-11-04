package com.vistay.notification.application;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.vistay.notification.application.NotificationContactResolverPort.ContactSnapshot;
import com.vistay.notification.application.NotificationContactResolverPort.VerificationStatus;
import com.vistay.notification.domain.recipient.RecipientKind;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** Contract-level cases shared by the persistence integration fixture and unit suite. */
class RecipientResolutionIntegrationTest {
    private final ObjectMapper json = JsonMapper.builder().build();

    @Test
    void classifiesDirectRoleBookingAndSubscriptionCandidatesWithoutReadingUserData() {
        assertEquals(RecipientKind.DIRECT_ACCOUNT,
            RecipientResolutionService.recipientKind(json.readTree("{}")));
        assertEquals(RecipientKind.ORGANIZATION_ROLE,
            RecipientResolutionService.recipientKind(json.readTree("{\"organizationId\":\"org\"}")));
        assertEquals(RecipientKind.BRANCH_ROLE,
            RecipientResolutionService.recipientKind(json.readTree("{\"branchId\":\"branch\"}")));
        assertEquals(RecipientKind.BOOKING_PARTY,
            RecipientResolutionService.recipientKind(json.readTree("{\"bookingId\":\"booking\"}")));
        assertEquals(RecipientKind.EXPLICIT_SUBSCRIPTION,
            RecipientResolutionService.recipientKind(json.readTree("{\"subscriptionId\":\"sub\"}")));
    }

    @Test
    void keepsContactDecisionCodesStableAndDoesNotExposeRawContactValues() {
        assertEquals("MISSING_CONTACT", RecipientResolutionService.contactCode(null));
        assertEquals("UNVERIFIED_CONTACT", RecipientResolutionService.contactCode(
            new ContactSnapshot(7, "***@example.test", new byte[] {1, 2},
                VerificationStatus.UNVERIFIED, "user-contact-api", null)));
        assertEquals("INVALID_CONTACT", RecipientResolutionService.contactCode(
            new ContactSnapshot(8, "***", new byte[] {3},
                VerificationStatus.INVALID, "user-contact-api", null)));
        assertEquals("INVALID_CONTACT", RecipientResolutionService.validateContactSnapshot(
            new ContactSnapshot(9, "***", new byte[] {3}, VerificationStatus.VERIFIED,
                "user-contact-api", "not-a-sha")));
        assertEquals("MISSING_CONTACT", RecipientResolutionService.validateContactSnapshot(
            new ContactSnapshot(10, null, null, VerificationStatus.VERIFIED,
                "user-contact-api", null)));
    }
}
