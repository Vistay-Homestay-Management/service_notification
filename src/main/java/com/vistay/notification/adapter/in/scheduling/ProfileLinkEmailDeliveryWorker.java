package com.vistay.notification.adapter.in.scheduling;

import com.vistay.notification.adapter.out.security.VaultProfileLinkDeliveryDecryptor;
import jakarta.mail.internet.MimeMessage;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Component
@ConditionalOnProperty(
    name = "vistay.notifications.profile-link-email-enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class ProfileLinkEmailDeliveryWorker {
    public static final String CHALLENGE_DELIVERED = "vistay.user.profile-link-challenge-delivered";

    private final JdbcClient jdbc;
    private final VaultProfileLinkDeliveryDecryptor decryptor;
    private final JavaMailSender mail;
    private final ObjectMapper json;
    private final String from;

    public ProfileLinkEmailDeliveryWorker(
        JdbcClient jdbc,
        VaultProfileLinkDeliveryDecryptor decryptor,
        JavaMailSender mail,
        ObjectMapper json,
        @Value("${vistay.notifications.profile-link-email-from:no-reply@vistay.localhost}") String from
    ) {
        this.jdbc = jdbc;
        this.decryptor = decryptor;
        this.mail = mail;
        this.json = json;
        this.from = from;
    }

    @Scheduled(fixedDelayString = "${vistay.notifications.profile-link-email-poll-delay-ms:1000}")
    @Transactional
    public void deliverPending() {
        for (var row : lockPending()) {
            deliver(row);
        }
    }

    private List<DeliveryRow> lockPending() {
        return jdbc.sql("""
            SELECT id, source_event_id, link_id, challenge_id, profile_type,
                   delivery_ciphertext, attempt_count, maximum_attempts, expires_at,
                   correlation_id, causation_id
            FROM profile_link_email_deliveries
            WHERE status IN ('PENDING', 'FAILED')
              AND attempt_count < maximum_attempts
              AND next_attempt_at <= now()
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 20
            """).query(DeliveryRow.class).list();
    }

    private void deliver(DeliveryRow row) {
        var now = OffsetDateTime.now(ZoneOffset.UTC);
        if (!row.expiresAt().isAfter(now)) {
            jdbc.sql("""
                UPDATE profile_link_email_deliveries
                SET status = 'EXPIRED', updated_at = now()
                WHERE id = :id
                """).param("id", row.id()).update();
            return;
        }
        var attempt = row.attemptCount() + 1;
        jdbc.sql("""
            UPDATE profile_link_email_deliveries
            SET status = 'SENDING', attempt_count = :attempt, updated_at = now()
            WHERE id = :id
            """).param("attempt", attempt).param("id", row.id()).update();
        try {
            var secret = parseSecret(decryptor.decrypt(row.deliveryCiphertext()), row.challengeId());
            var providerMessageId = "profile-link-" + row.sourceEventId();
            send(secret.email(), secret.otp(), providerMessageId, row.expiresAt());
            jdbc.sql("""
                UPDATE profile_link_email_deliveries
                SET status = 'SENT', provider_message_id = :providerMessageId,
                    sent_at = now(), last_error_code = NULL, last_error_message = NULL,
                    updated_at = now()
                WHERE id = :id
                """)
                .param("providerMessageId", providerMessageId).param("id", row.id()).update();
            outbox(row, providerMessageId, now, attempt);
        } catch (RuntimeException failure) {
            jdbc.sql("""
                UPDATE profile_link_email_deliveries
                SET status = 'FAILED', next_attempt_at = now() + (:attempt * interval '15 seconds'),
                    last_error_code = :errorCode,
                    last_error_message = 'Profile link email delivery failed',
                    updated_at = now()
                WHERE id = :id
                """)
                .param("attempt", attempt)
                .param("errorCode", failure.getClass().getSimpleName())
                .param("id", row.id()).update();
        }
    }

    private void send(String recipient, String otp, String providerMessageId, OffsetDateTime expiresAt) {
        try {
            MimeMessage message = mail.createMimeMessage();
            message.setHeader("Message-ID", "<" + providerMessageId + "@vistay.localhost>");
            var helper = new MimeMessageHelper(message, "UTF-8");
            helper.setFrom(from);
            helper.setTo(recipient);
            helper.setSubject("Mã xác nhận liên kết hồ sơ ViStay");
            helper.setText("Mã xác nhận của bạn là " + otp
                + ". Mã hết hạn lúc " + expiresAt + ". Nếu bạn không yêu cầu, hãy bỏ qua email này.", false);
            mail.send(message);
        } catch (Exception failure) {
            throw new EmailDeliveryFailed("SMTP delivery failed", failure);
        }
    }

    private void outbox(DeliveryRow row, String providerMessageId, OffsetDateTime deliveredAt, int attempt) {
        try {
            var payload = json.writeValueAsString(Map.of(
                "linkId", row.linkId(),
                "challengeId", row.challengeId(),
                "profileType", row.profileType(),
                "provider", "SMTP",
                "providerMessageId", providerMessageId,
                "deliveredAt", deliveredAt
            ));
            jdbc.sql("""
                INSERT INTO outbox_events(
                    aggregate_type, aggregate_id, aggregate_version, event_type,
                    event_version, payload, correlation_id, causation_id
                ) VALUES (
                    'ProfileLinkChallenge', :challengeId, :version, :eventType,
                    1, CAST(:payload AS jsonb), :correlationId, :causationId
                )
                """)
                .param("challengeId", row.challengeId()).param("version", attempt)
                .param("eventType", CHALLENGE_DELIVERED).param("payload", payload)
                .param("correlationId", row.correlationId()).param("causationId", row.sourceEventId().toString())
                .update();
        } catch (Exception failure) {
            throw new EmailDeliveryFailed("Cannot persist delivery acknowledgement", failure);
        }
    }

    private static DeliverySecret parseSecret(String plaintext, UUID expectedChallengeId) {
        var parts = plaintext.split("\\n", -1);
        if (parts.length != 4 || !"profile-link-email-v1".equals(parts[0])) {
            throw new IllegalArgumentException("Unsupported profile link delivery secret");
        }
        if (!expectedChallengeId.equals(UUID.fromString(parts[1]))) {
            throw new IllegalArgumentException("Profile link delivery challenge mismatch");
        }
        if (!parts[2].matches("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$") || !parts[3].matches("^[0-9]{6}$")) {
            throw new IllegalArgumentException("Invalid profile link delivery secret");
        }
        return new DeliverySecret(parts[2], parts[3]);
    }

    private record DeliverySecret(String email, String otp) {}
    private record DeliveryRow(UUID id, UUID sourceEventId, UUID linkId, UUID challengeId,
        String profileType, String deliveryCiphertext, int attemptCount, int maximumAttempts,
        OffsetDateTime expiresAt, String correlationId, String causationId) {}

    private static final class EmailDeliveryFailed extends RuntimeException {
        private EmailDeliveryFailed(String message, Throwable cause) { super(message, cause); }
    }
}
