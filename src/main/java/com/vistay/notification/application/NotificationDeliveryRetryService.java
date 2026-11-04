package com.vistay.notification.application;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Bounded delivery scheduler, provider circuit state, dead-letter lifecycle,
 * and auditable repair commands. Raw destinations never leave the database.
 */
@Service
public class NotificationDeliveryRetryService {
    private static final Duration LEASE_DURATION = Duration.ofSeconds(30);
    private static final String DELIVERY_STATE_CHANGED =
        "vistay.notification.delivery-state-changed";

    private final JdbcClient jdbc;
    private final ObjectMapper json;
    private final List<NotificationProviderPort> providers;
    private final Duration providerTimeout;

    public NotificationDeliveryRetryService(
            JdbcClient jdbc,
            ObjectMapper json,
            List<NotificationProviderPort> providers) {
        this(jdbc, json, providers, Duration.ofSeconds(2));
    }

    @Autowired
    public NotificationDeliveryRetryService(
            JdbcClient jdbc,
            ObjectMapper json,
            List<NotificationProviderPort> providers,
            @Value("${vistay.notifications.provider-timeout:2s}") Duration providerTimeout) {
        this.jdbc = jdbc;
        this.json = json;
        this.providers = providers == null ? List.of() : List.copyOf(providers);
        this.providerTimeout = providerTimeout;
    }

    /** Claims expired/retryable deliveries with SKIP LOCKED and processes them. */
    @Transactional
    public int processPending(int limit) {
        recoverExpiredLeases();
        int boundedLimit = Math.max(1, Math.min(limit, 100));
        List<DeliveryRow> rows = jdbc.sql("""
                SELECT d.id, d.notification_id, d.channel, d.provider,
                       d.provider_idempotency_key, d.destination_masked,
                       d.destination_ciphertext, d.attempt_count, d.maximum_attempts,
                       d.delivery_revision, d.first_failure_at, d.max_age_until,
                       d.retry_policy_key, d.retry_policy_revision, n.title, n.body
                FROM notification_deliveries d
                JOIN notifications n ON n.id = d.notification_id
                WHERE d.status IN ('PENDING', 'FAILED')
                  AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= now())
                  AND (d.retry_after_at IS NULL OR d.retry_after_at <= now())
                  AND (d.lease_expires_at IS NULL OR d.lease_expires_at <= now())
                ORDER BY d.created_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """)
            .param("limit", boundedLimit)
            .query(DeliveryRow.class)
            .list();
        int processed = 0;
        for (DeliveryRow row : rows) {
            if (dispatch(row)) {
                processed++;
            }
        }
        return processed;
    }

    @Transactional(readOnly = true)
    public DeliveryView getDelivery(UUID deliveryId) {
        return jdbc.sql(deliverySelect() + " WHERE d.id = :deliveryId")
            .param("deliveryId", deliveryId)
            .query(DeliveryView.class)
            .list()
            .stream()
            .findFirst()
            .orElseThrow(() -> new NotificationCommandException("NOT_FOUND", "Notification delivery was not found"));
    }

    @Transactional(readOnly = true)
    public List<ProviderHealthView> providerHealth() {
        return jdbc.sql("""
                SELECT provider, status, circuit_state, failure_count,
                       CASE WHEN failure_count + success_count = 0 THEN 0.0
                            ELSE failure_count::double precision /
                                (failure_count + success_count) END AS failure_rate,
                       latency_milliseconds, retry_after_seconds, checked_at
                FROM notification_provider_state
                ORDER BY provider
                """)
            .query(ProviderHealthView.class)
            .list();
    }

    @Transactional
    public DeliveryView repair(RepairCommand command) {
        validateRepair(command);
        String fingerprint = DomainEventNotificationService.fingerprint(
            command.action().name(), command.deliveryId().toString(),
            Integer.toString(command.expectedVersion()), command.reason(), command.provider());
        RepairRow previous = findRepair(command.action(), command.idempotencyKey());
        if (previous != null) {
            if (!fingerprint.equals(previous.requestFingerprint())) {
                throw new NotificationCommandException("IDEMPOTENCY_KEY_REUSED",
                    "Idempotency key was already used for another repair");
            }
            return getDelivery(previous.deliveryId());
        }

        DeliveryView before = loadForUpdate(command.deliveryId());
        if (before.revision() != command.expectedVersion()) {
            throw new NotificationCommandException("STALE_REVISION",
                "Delivery revision does not match the expected version");
        }
        if (command.action() == RepairAction.REROUTE && blank(command.provider())) {
            throw new NotificationCommandException("VALIDATION_ERROR", "A reroute provider is required");
        }
        if (command.action() == RepairAction.REROUTE
                && providers.stream().noneMatch(provider -> provider.provider().equals(command.provider()))) {
            throw new NotificationCommandException("PROVIDER_UNAVAILABLE", "Requested provider is unavailable");
        }

        DeliveryView after = switch (command.action()) {
            case RETRY_NOW -> retryNow(before);
            case REROUTE -> reroute(before, command.provider());
            case CANCEL -> cancel(before);
            case RESOLVE_INVALID_CONTACT -> resolveInvalidContact(before);
            case REPLAY_SOURCE -> replaySource(before);
        };
        if (command.action() == RepairAction.RETRY_NOW
                || command.action() == RepairAction.REROUTE
                || command.action() == RepairAction.RESOLVE_INVALID_CONTACT
                || command.action() == RepairAction.REPLAY_SOURCE) {
            jdbc.sql("""
                    UPDATE notification_dead_letters
                    SET state = 'RETRY_SCHEDULED', revision = revision + 1, updated_at = now()
                    WHERE delivery_id = :deliveryId AND state IN ('OPEN', 'IN_REVIEW')
                    """)
                .param("deliveryId", command.deliveryId()).update();
        }
        String beforeSnapshot = snapshot(before);
        String afterSnapshot = snapshot(after);
        jdbc.sql("""
                INSERT INTO notification_repair_decisions(
                    dead_letter_id, delivery_id, action, reason,
                    expected_delivery_revision, before_snapshot, after_snapshot,
                    actor_account_type, actor_account_id, idempotency_key
                ) VALUES (
                    (SELECT id FROM notification_dead_letters WHERE delivery_id = :deliveryId),
                    :deliveryId, :action, :reason, :expectedVersion,
                    CAST(:beforeSnapshot AS jsonb), CAST(:afterSnapshot AS jsonb),
                    :actorType, :actorId, :idempotencyKey
                )
                """)
            .param("deliveryId", command.deliveryId())
            .param("action", command.action().name())
            .param("reason", command.reason())
            .param("expectedVersion", command.expectedVersion())
            .param("beforeSnapshot", beforeSnapshot)
            .param("afterSnapshot", afterSnapshot)
            .param("actorType", command.actorAccountType())
            .param("actorId", command.actorAccountId())
            .param("idempotencyKey", command.idempotencyKey())
            .update();
        writeAudit(command, after);
        writeOutbox(command, after);
        return after;
    }

    private boolean dispatch(DeliveryRow row) {
        NotificationRetryPolicy.Policy policy = policy(row.retryPolicyKey(), row.retryPolicyRevision());
        String providerName = providerName(row);
        if (providerName != null && circuitOpen(providerName)) {
            deferForCircuit(row, providerName);
            return false;
        }
        UUID leaseId = UUID.randomUUID();
        if (!claim(row, leaseId)) {
            return false;
        }
        if (NotificationChannelDeliveryPolicy.IN_APP.equals(row.channel())) {
            complete(row, leaseId, "SENT", null, null);
            return true;
        }
        if (row.destinationCiphertext() == null || row.destinationCiphertext().length == 0) {
            fail(row, leaseId, policy, "MISSING_CONTACT", "INVALID_CONTACT", null, null);
            return true;
        }
        NotificationProviderPort provider = providers.stream()
            .filter(candidate -> row.channel().equals(candidate.channel()))
            .findFirst()
            .orElse(null);
        if (provider == null) {
            fail(row, leaseId, policy, "PROVIDER_UNAVAILABLE", "PROVIDER_OUTAGE", null, "provider-missing");
            return true;
        }
        try {
            heartbeat(row.id(), leaseId);
            long startedNanos = System.nanoTime();
            NotificationProviderPort.SendResult result = provider.send(new NotificationProviderPort.SendRequest(
                row.id(), row.channel(), row.providerIdempotencyKey(), row.destinationMasked(),
                row.destinationCiphertext(), row.title(), row.body(), providerTimeout));
            long latencyMilliseconds = Math.max(0,
                Duration.ofNanos(System.nanoTime() - startedNanos).toMillis());
            if (result.outcome() == NotificationProviderPort.Outcome.ACCEPTED) {
                complete(row, leaseId, "ACCEPTED", provider.provider(), result.providerMessageId());
                recordProviderLatency(provider.provider(), latencyMilliseconds);
            } else {
                fail(row, leaseId, policy, result.errorCode(), result.errorClass(), result.retryAt(), provider.provider());
                recordProviderLatency(provider.provider(), latencyMilliseconds);
            }
        } catch (RuntimeException providerFailure) {
            fail(row, leaseId, policy, "PROVIDER_UNAVAILABLE", "PROVIDER_OUTAGE", null, provider.provider());
        }
        return true;
    }

    private boolean claim(DeliveryRow row, UUID leaseId) {
        return jdbc.sql("""
                UPDATE notification_deliveries
                SET status = 'SENDING', lease_id = :leaseId,
                    lease_expires_at = now() + (:leaseSeconds * interval '1 second'),
                    lease_heartbeat_at = now(), first_attempt_at = COALESCE(first_attempt_at, now()),
                    attempt_count = attempt_count + 1, updated_at = now()
                WHERE id = :id AND delivery_revision = :revision
                  AND status IN ('PENDING', 'FAILED')
                  AND attempt_count < maximum_attempts
                  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
                """)
            .param("leaseId", leaseId)
            .param("leaseSeconds", LEASE_DURATION.toSeconds())
            .param("id", row.id())
            .param("revision", row.deliveryRevision())
            .update() == 1;
    }

    private void heartbeat(UUID deliveryId, UUID leaseId) {
        jdbc.sql("""
                UPDATE notification_deliveries
                SET lease_heartbeat_at = now(), lease_expires_at = now() + (:leaseSeconds * interval '1 second')
                WHERE id = :deliveryId AND lease_id = :leaseId AND status = 'SENDING'
                """)
            .param("deliveryId", deliveryId)
            .param("leaseId", leaseId)
            .param("leaseSeconds", LEASE_DURATION.toSeconds())
            .update();
    }

    private void complete(DeliveryRow row, UUID leaseId, String status, String provider, String messageId) {
        jdbc.sql("""
                UPDATE notification_deliveries
                SET status = :status, provider = COALESCE(:provider, provider),
                    provider_message_id = :messageId, accepted_at = COALESCE(accepted_at, now()),
                    sent_at = COALESCE(sent_at, now()), lease_id = NULL,
                    lease_expires_at = NULL, lease_heartbeat_at = NULL,
                    last_error_code = NULL, error_class = NULL, updated_at = now(),
                    delivery_revision = delivery_revision + 1
                WHERE id = :id AND lease_id = :leaseId AND status = 'SENDING'
                """)
            .param("status", status)
            .param("provider", provider)
            .param("messageId", messageId)
            .param("id", row.id())
            .param("leaseId", leaseId)
            .update();
        insertAttempt(row, "ACCEPTED", null, null);
        if (provider != null) {
            recordProviderSuccess(provider);
        }
        writeStateOutbox(row, status, null);
    }

    private void fail(
            DeliveryRow row,
            UUID leaseId,
            NotificationRetryPolicy.Policy policy,
            String errorCode,
            String errorClass,
            OffsetDateTime retryAt,
            String provider) {
        NotificationRetryPolicy.ErrorClass classified = NotificationRetryPolicy.classify(errorCode, errorClass);
        Instant now = Instant.now();
        Instant firstFailure = row.firstFailureAt() == null
            ? now : row.firstFailureAt().toInstant();
        int attemptCount = row.attemptCount() + 1;
        boolean exhausted = !NotificationRetryPolicy.retryable(classified)
            || policy.exhausted(attemptCount, firstFailure, now)
            || (row.maxAgeUntil() != null && !now.isBefore(row.maxAgeUntil().toInstant()));
        Instant nextAttempt = retryAt == null
            ? policy.nextAttemptAt(now, attemptCount,
                jitteredRetryAfter(policy, now, attemptCount))
            : retryAt.toInstant();
        if (exhausted) {
            jdbc.sql("""
                    UPDATE notification_deliveries
                    SET status = 'DEAD_LETTERED', failed_at = now(), dead_lettered_at = now(),
                        terminal_reason = :terminalReason, last_error_code = :errorCode,
                        error_class = :errorClass, lease_id = NULL, lease_expires_at = NULL,
                        lease_heartbeat_at = NULL, delivery_revision = delivery_revision + 1,
                        updated_at = now()
                    WHERE id = :id AND lease_id = :leaseId AND status = 'SENDING'
                    """)
                .param("terminalReason", errorCode == null ? "RETRY_EXHAUSTED" : errorCode)
                .param("errorCode", errorCode)
                .param("errorClass", classified.name())
                .param("id", row.id())
                .param("leaseId", leaseId)
                .update();
            insertDeadLetter(row, errorCode == null ? "RETRY_EXHAUSTED" : errorCode, classified, attemptCount);
        } else {
            jdbc.sql("""
                    UPDATE notification_deliveries
                    SET status = 'FAILED', failed_at = now(), first_failure_at = COALESCE(first_failure_at, now()),
                        next_attempt_at = :nextAttempt, retry_after_at = :nextAttempt,
                        last_error_code = :errorCode, error_class = :errorClass,
                        lease_id = NULL, lease_expires_at = NULL, lease_heartbeat_at = NULL,
                        delivery_revision = delivery_revision + 1, updated_at = now()
                    WHERE id = :id AND lease_id = :leaseId AND status = 'SENDING'
                    """)
                .param("nextAttempt", OffsetDateTime.ofInstant(nextAttempt, ZoneOffset.UTC))
                .param("errorCode", errorCode)
                .param("errorClass", classified.name())
                .param("id", row.id())
                .param("leaseId", leaseId)
                .update();
        }
        insertAttempt(row, classified == NotificationRetryPolicy.ErrorClass.RATE_LIMITED
            ? "RATE_LIMITED" : "FAILED", errorCode, classified.name());
        if (provider != null) {
            recordProviderFailure(provider, policy, errorCode);
        }
        writeStateOutbox(row, exhausted ? "DEAD_LETTERED" : "FAILED", errorCode);
    }

    private Instant jitteredRetryAfter(NotificationRetryPolicy.Policy policy, Instant now, int attempt) {
        double factor = policy.jitterPercent() == 0 ? 0
            : ThreadLocalRandom.current().nextDouble(-policy.jitterPercent(), policy.jitterPercent()) / 100;
        return now.plus(policy.backoffWithJitter(attempt, factor));
    }

    private void insertAttempt(DeliveryRow row, String result, String code, String message) {
        jdbc.sql("""
                INSERT INTO delivery_attempts(
                    delivery_id, attempt_number, result, error_code, error_message
                ) VALUES (:deliveryId, :attemptNumber, :result, :errorCode, :errorMessage)
                ON CONFLICT (delivery_id, attempt_number) DO NOTHING
                """)
            .param("deliveryId", row.id())
            .param("attemptNumber", row.attemptCount() + 1)
            .param("result", result)
            .param("errorCode", code)
            .param("errorMessage", message)
            .update();
    }

    private void insertDeadLetter(
            DeliveryRow row,
            String reason,
            NotificationRetryPolicy.ErrorClass errorClass,
            int attemptCount) {
        jdbc.sql("""
                INSERT INTO notification_dead_letters(
                    delivery_id, notification_id, reason_code, error_class, attempt_count,
                    source_event_id, source_event_type
                )
                SELECT :deliveryId, n.id, :reason, :errorClass, :attemptCount,
                       n.source_event_id, n.source_event_type
                FROM notifications n WHERE n.id = :notificationId
                ON CONFLICT (delivery_id) DO UPDATE SET
                    reason_code = EXCLUDED.reason_code, error_class = EXCLUDED.error_class,
                    attempt_count = EXCLUDED.attempt_count, last_failure_at = now(), updated_at = now()
                """)
            .param("deliveryId", row.id())
            .param("notificationId", row.notificationId())
            .param("reason", reason)
            .param("errorClass", errorClass.name())
            .param("attemptCount", attemptCount)
            .update();
    }

    private void recoverExpiredLeases() {
        jdbc.sql("""
                UPDATE notification_deliveries
                SET status = CASE WHEN attempt_count >= maximum_attempts THEN 'FAILED' ELSE 'PENDING' END,
                    lease_id = NULL, lease_expires_at = NULL, lease_heartbeat_at = NULL,
                    next_attempt_at = COALESCE(next_attempt_at, now()), updated_at = now()
                WHERE status = 'SENDING' AND lease_expires_at < now()
                """).update();
    }

    private boolean circuitOpen(String provider) {
        ProviderCircuit row = jdbc.sql("""
                SELECT circuit_state, open_until
                FROM notification_provider_state WHERE provider = :provider
                """)
            .param("provider", provider)
            .query(ProviderCircuit.class)
            .list()
            .stream().findFirst().orElse(null);
        return row != null && "OPEN".equals(row.circuitState())
            && row.openUntil() != null && row.openUntil().isAfter(OffsetDateTime.now());
    }

    private void deferForCircuit(DeliveryRow row, String provider) {
        OffsetDateTime retryAt = jdbc.sql("""
                SELECT COALESCE(open_until, now() + interval '30 seconds')
                FROM notification_provider_state WHERE provider = :provider
                """)
            .param("provider", provider)
            .query(OffsetDateTime.class)
            .list().stream().findFirst().orElse(OffsetDateTime.now().plusSeconds(30));
        jdbc.sql("""
                UPDATE notification_deliveries
                SET next_attempt_at = :retryAt, retry_after_at = :retryAt,
                    last_error_code = 'PROVIDER_CIRCUIT_OPEN', error_class = 'PROVIDER_OUTAGE',
                    updated_at = now()
                WHERE id = :id AND status IN ('PENDING', 'FAILED')
                """)
            .param("retryAt", retryAt)
            .param("id", row.id())
            .update();
    }

    private void recordProviderSuccess(String provider) {
        jdbc.sql("""
                UPDATE notification_provider_state
                SET status = 'HEALTHY', circuit_state = 'CLOSED', consecutive_failures = 0,
                    success_count = success_count + 1, open_until = NULL,
                    retry_after_seconds = NULL, checked_at = now(), revision = revision + 1
                WHERE provider = :provider
                """)
            .param("provider", provider).update();
    }

    private void recordProviderLatency(String provider, long latencyMilliseconds) {
        jdbc.sql("""
                UPDATE notification_provider_state
                SET latency_milliseconds = :latencyMilliseconds,
                    status = CASE WHEN :latencyMilliseconds > latency_threshold_milliseconds
                        AND status = 'HEALTHY' THEN 'DEGRADED' ELSE status END,
                    checked_at = now(), revision = revision + 1
                WHERE provider = :provider
                """)
            .param("latencyMilliseconds", latencyMilliseconds)
            .param("provider", provider)
            .update();
    }

    private void recordProviderFailure(String provider, NotificationRetryPolicy.Policy policy, String code) {
        OffsetDateTime openUntil = OffsetDateTime.now().plus(policy.circuitOpenDuration());
        jdbc.sql("""
                UPDATE notification_provider_state
                SET consecutive_failures = consecutive_failures + 1,
                    failure_count = failure_count + 1, last_error_code = :errorCode,
                    status = CASE WHEN consecutive_failures + 1 >= :threshold
                        THEN 'UNAVAILABLE' ELSE 'DEGRADED' END,
                    circuit_state = CASE WHEN consecutive_failures + 1 >= :threshold
                        THEN 'OPEN' ELSE circuit_state END,
                    opened_at = CASE WHEN consecutive_failures + 1 >= :threshold
                        THEN COALESCE(opened_at, now()) ELSE opened_at END,
                    open_until = CASE WHEN consecutive_failures + 1 >= :threshold
                        THEN :openUntil ELSE open_until END,
                    retry_after_seconds = CASE WHEN consecutive_failures + 1 >= :threshold
                        THEN :openSeconds ELSE retry_after_seconds END,
                    checked_at = now(), revision = revision + 1
                WHERE provider = :provider
                """)
            .param("errorCode", code)
            .param("threshold", policy.circuitFailureThreshold())
            .param("openUntil", openUntil)
            .param("openSeconds", policy.circuitOpenDuration().toSeconds())
            .param("provider", provider)
            .update();
    }

    private NotificationRetryPolicy.Policy policy(String key, int revision) {
        RetryPolicyRow row = jdbc.sql("""
                SELECT policy_key, revision, max_attempts, max_age_seconds,
                       initial_delay_seconds, maximum_delay_seconds, jitter_percent,
                       circuit_failure_threshold, circuit_open_seconds,
                       latency_threshold_milliseconds
                FROM notification_retry_policies
                WHERE policy_key = :policyKey AND revision = :revision
                """)
            .param("policyKey", key)
            .param("revision", revision)
            .query(RetryPolicyRow.class)
            .list().stream().findFirst().orElse(null);
        if (row == null) {
            row = new RetryPolicyRow("notification.default", 1, 5, 86400, 30, 3600, 20, 5, 30, 2000);
        }
        return new NotificationRetryPolicy.Policy(row.policyKey(), row.revision(), row.maxAttempts(),
            Duration.ofSeconds(row.maxAgeSeconds()), Duration.ofSeconds(row.initialDelaySeconds()),
            Duration.ofSeconds(row.maximumDelaySeconds()), row.jitterPercent(),
            row.circuitFailureThreshold(), Duration.ofSeconds(row.circuitOpenSeconds()),
            Duration.ofMillis(row.latencyThresholdMilliseconds()));
    }

    private DeliveryView loadForUpdate(UUID deliveryId) {
        return jdbc.sql(deliverySelect() + " WHERE d.id = :deliveryId FOR UPDATE")
            .param("deliveryId", deliveryId)
            .query(DeliveryView.class)
            .list().stream().findFirst()
            .orElseThrow(() -> new NotificationCommandException("NOT_FOUND", "Notification delivery was not found"));
    }

    private DeliveryView retryNow(DeliveryView before) {
        if (terminal(before.status())) {
            throw new NotificationCommandException("CONFLICT", "Delivery cannot be retried in its current state");
        }
        return updateRepairState(before, "PENDING", null, null, null);
    }

    private DeliveryView reroute(DeliveryView before, String provider) {
        if (terminal(before.status())) {
            throw new NotificationCommandException("CONFLICT", "Delivery cannot be rerouted in its current state");
        }
        return updateRepairState(before, "PENDING", provider, null, null);
    }

    private DeliveryView cancel(DeliveryView before) {
        if ("DELIVERED".equals(before.status()) || "READ".equals(before.status())) {
            throw new NotificationCommandException("CONFLICT", "Delivered notification cannot be cancelled");
        }
        DeliveryView after = updateRepairState(before, "CANCELLED", null, "CANCELLED_BY_OPERATOR", null);
        jdbc.sql("""
                UPDATE notification_dead_letters
                SET state = 'CANCELLED', resolved_at = now(), resolution_action = 'CANCEL',
                    revision = revision + 1, updated_at = now()
                WHERE delivery_id = :deliveryId AND state <> 'RESOLVED'
                """)
            .param("deliveryId", before.id()).update();
        return after;
    }

    private DeliveryView resolveInvalidContact(DeliveryView before) {
        if (terminal(before.status()) && !"DEAD_LETTERED".equals(before.status())) {
            throw new NotificationCommandException("CONFLICT", "Delivery cannot be contact-repaired in its current state");
        }
        return updateRepairState(before, "PENDING", null, null, "PENDING");
    }

    private DeliveryView replaySource(DeliveryView before) {
        if (terminal(before.status()) && !"DEAD_LETTERED".equals(before.status())) {
            throw new NotificationCommandException("CONFLICT", "Delivery source cannot be replayed in its current state");
        }
        return updateRepairState(before, "PENDING", null, null, null);
    }

    private DeliveryView updateRepairState(
            DeliveryView before,
            String status,
            String provider,
            String terminalReason,
            String resolutionState) {
        jdbc.sql("""
                UPDATE notification_deliveries
                SET status = :status, provider = COALESCE(:provider, provider),
                    terminal_reason = :terminalReason,
                    destination_resolution_state = COALESCE(:resolutionState, destination_resolution_state),
                    next_attempt_at = CASE WHEN :status = 'PENDING' THEN now() ELSE next_attempt_at END,
                    retry_after_at = CASE WHEN :status = 'PENDING' THEN now() ELSE retry_after_at END,
                    failed_at = CASE WHEN :status = 'PENDING' THEN NULL ELSE failed_at END,
                    dead_lettered_at = CASE WHEN :status = 'PENDING' THEN NULL ELSE dead_lettered_at END,
                    delivery_revision = delivery_revision + 1, updated_at = now()
                WHERE id = :id AND delivery_revision = :revision
                """)
            .param("status", status)
            .param("provider", provider)
            .param("terminalReason", terminalReason)
            .param("resolutionState", resolutionState)
            .param("id", before.id())
            .param("revision", before.revision())
            .update();
        return getDelivery(before.id());
    }

    private void writeAudit(RepairCommand command, DeliveryView after) {
        jdbc.sql("""
                INSERT INTO audit_events(
                    actor_admin_user_id, action, aggregate_type, aggregate_id, changes
                ) VALUES (:actorId, :action, 'NotificationDelivery', :deliveryId,
                    CAST(:changes AS jsonb))
                """)
            .param("actorId", command.actorAccountId())
            .param("action", "NOTIFICATION_DELIVERY_" + command.action().name())
            .param("deliveryId", after.id())
            .param("changes", snapshot(after))
            .update();
    }

    private void writeOutbox(RepairCommand command, DeliveryView after) {
        jdbc.sql("""
                INSERT INTO outbox_events(
                    aggregate_type, aggregate_id, aggregate_version, event_type,
                    event_version, payload, causation_id
                ) VALUES ('NotificationDelivery', :deliveryId, :revision,
                    'vistay.notification.delivery-repair', 1, CAST(:payload AS jsonb),
                    :causationId)
                """)
            .param("deliveryId", after.id())
            .param("revision", after.revision())
            .param("payload", snapshot(after))
            .param("causationId", command.idempotencyKey())
            .update();
    }

    private void writeStateOutbox(DeliveryRow row, String status, String errorCode) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("deliveryId", row.id());
        payload.put("notificationId", row.notificationId());
        payload.put("channel", row.channel());
        payload.put("status", status);
        payload.put("errorCode", errorCode);
        jdbc.sql("""
                INSERT INTO outbox_events(
                    aggregate_type, aggregate_id, aggregate_version, event_type,
                    event_version, payload
                ) VALUES ('NotificationDelivery', :deliveryId, :aggregateVersion,
                    :eventType, 1, CAST(:payload AS jsonb))
                """)
            .param("deliveryId", row.id())
            .param("aggregateVersion", row.deliveryRevision() + 1L)
            .param("eventType", DELIVERY_STATE_CHANGED)
            .param("payload", jsonObject(payload))
            .update();
    }

    private String snapshot(DeliveryView view) {
        Map<String, Object> safe = new LinkedHashMap<>();
        safe.put("deliveryId", view.id());
        safe.put("notificationId", view.notificationId());
        safe.put("channel", view.channel());
        safe.put("status", view.status());
        safe.put("provider", view.provider());
        safe.put("attemptCount", view.attemptCount());
        safe.put("revision", view.revision());
        safe.put("lastErrorCode", view.lastErrorCode());
        return jsonObject(safe);
    }

    private String jsonObject(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception serializationFailure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Delivery metadata cannot be serialized");
        }
    }

    private RepairRow findRepair(RepairAction action, String key) {
        return jdbc.sql("""
                SELECT request_fingerprint, delivery_id
                FROM notification_repair_decisions
                WHERE action = :action AND idempotency_key = :idempotencyKey
                """)
            .param("action", action.name())
            .param("idempotencyKey", key)
            .query(RepairRow.class)
            .list().stream().findFirst().orElse(null);
    }

    private static boolean terminal(String status) {
        return "ACCEPTED".equals(status) || "SENT".equals(status)
            || "DELIVERED".equals(status) || "READ".equals(status)
            || "CANCELLED".equals(status);
    }

    private static String providerName(DeliveryRow row) {
        if (row.provider() != null && !row.provider().isBlank()) {
            return row.provider();
        }
        return switch (row.channel()) {
            case "EMAIL" -> "email";
            case "PUSH" -> "push";
            default -> null;
        };
    }

    private static void validateRepair(RepairCommand command) {
        if (command == null || command.deliveryId() == null || command.expectedVersion() < 1
                || blank(command.reason()) || command.reason().length() > 1000
                || blank(command.idempotencyKey()) || blank(command.actorAccountType())
                || command.actorAccountId() == null || command.action() == null) {
            throw new NotificationCommandException("VALIDATION_ERROR", "Delivery repair input is invalid");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private String deliverySelect() {
        return """
            SELECT d.id, d.notification_id, d.channel, d.status, d.provider,
                   d.template_revision, d.policy_revision, d.destination_masked,
                   d.contact_snapshot_revision, d.attempt_count, d.maximum_attempts,
                   d.next_attempt_at, d.last_error_code, d.error_class,
                   d.provider_message_id, d.delivery_revision AS revision, d.updated_at
            FROM notification_deliveries d
            """;
    }

    public enum RepairAction {
        RETRY_NOW,
        REROUTE,
        CANCEL,
        RESOLVE_INVALID_CONTACT,
        REPLAY_SOURCE
    }

    public record RepairCommand(
            UUID deliveryId,
            RepairAction action,
            int expectedVersion,
            String reason,
            String provider,
            String idempotencyKey,
            String actorAccountType,
            UUID actorAccountId) {}

    public record DeliveryView(
            UUID id,
            UUID notificationId,
            String channel,
            String status,
            String provider,
            int templateRevision,
            int policyRevision,
            String destinationMasked,
            Long contactSnapshotRevision,
            int attemptCount,
            int maximumAttempts,
            OffsetDateTime nextAttemptAt,
            String lastErrorCode,
            String lastErrorClass,
            String providerMessageId,
            int revision,
            OffsetDateTime updatedAt) {}

    public record ProviderHealthView(
            String provider,
            String status,
            String circuitState,
            int failureCount,
            double failureRate,
            int latencyMilliseconds,
            Integer retryAfterSeconds,
            OffsetDateTime checkedAt) {}

    private record DeliveryRow(
            UUID id,
            UUID notificationId,
            String channel,
            String provider,
            String providerIdempotencyKey,
            String destinationMasked,
            byte[] destinationCiphertext,
            int attemptCount,
            int maximumAttempts,
            int deliveryRevision,
            OffsetDateTime firstFailureAt,
            OffsetDateTime maxAgeUntil,
            String retryPolicyKey,
            int retryPolicyRevision,
            String title,
            String body) {}

    private record RetryPolicyRow(
            String policyKey,
            int revision,
            int maxAttempts,
            int maxAgeSeconds,
            int initialDelaySeconds,
            int maximumDelaySeconds,
            int jitterPercent,
            int circuitFailureThreshold,
            int circuitOpenSeconds,
            int latencyThresholdMilliseconds) {}

    private record ProviderCircuit(String circuitState, OffsetDateTime openUntil) {}

    private record RepairRow(String requestFingerprint, UUID deliveryId) {}
}
