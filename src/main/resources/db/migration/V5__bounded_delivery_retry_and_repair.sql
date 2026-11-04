SET search_path TO app, extensions, public;

-- V5 adds bounded execution state without rewriting the V2/V3 delivery rows.
ALTER TABLE notification_deliveries
    ADD COLUMN retry_policy_key varchar(100) NOT NULL DEFAULT 'notification.default',
    ADD COLUMN retry_policy_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN retry_policy_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN lease_id uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN lease_heartbeat_at timestamptz,
    ADD COLUMN first_attempt_at timestamptz,
    ADD COLUMN first_failure_at timestamptz,
    ADD COLUMN retry_after_at timestamptz,
    ADD COLUMN max_age_until timestamptz,
    ADD COLUMN dead_lettered_at timestamptz,
    ADD COLUMN terminal_reason varchar(160),
    ADD CONSTRAINT ck_notification_delivery_retry_policy_revision
        CHECK (retry_policy_revision > 0),
    ADD CONSTRAINT ck_notification_delivery_retry_policy_snapshot
        CHECK (jsonb_typeof(retry_policy_snapshot) = 'object');

ALTER TABLE notification_deliveries
    DROP CONSTRAINT ck_notification_delivery_status,
    ADD CONSTRAINT ck_notification_delivery_status_v5 CHECK (
        status IN (
            'PENDING', 'SENDING', 'ACCEPTED', 'SENT', 'DELIVERED', 'READ',
            'FAILED', 'BOUNCED', 'DEAD_LETTERED', 'CANCELLED'
        )
    );

CREATE INDEX ix_notification_delivery_claim_v5
    ON notification_deliveries(status, next_attempt_at, lease_expires_at, created_at)
    WHERE status IN ('PENDING', 'FAILED', 'SENDING');

CREATE TABLE notification_retry_policies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_key varchar(100) NOT NULL,
    revision integer NOT NULL,
    max_attempts integer NOT NULL,
    max_age_seconds integer NOT NULL,
    initial_delay_seconds integer NOT NULL,
    maximum_delay_seconds integer NOT NULL,
    jitter_percent integer NOT NULL DEFAULT 20,
    circuit_failure_threshold integer NOT NULL DEFAULT 5,
    circuit_open_seconds integer NOT NULL DEFAULT 30,
    latency_threshold_milliseconds integer NOT NULL DEFAULT 2000,
    immutable boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_retry_policy_version UNIQUE (policy_key, revision),
    CONSTRAINT ck_notification_retry_policy_revision CHECK (revision > 0),
    CONSTRAINT ck_notification_retry_policy_attempts CHECK (max_attempts > 0),
    CONSTRAINT ck_notification_retry_policy_age CHECK (max_age_seconds > 0),
    CONSTRAINT ck_notification_retry_policy_delays CHECK (
        initial_delay_seconds >= 0 AND maximum_delay_seconds >= initial_delay_seconds
    ),
    CONSTRAINT ck_notification_retry_policy_jitter CHECK (jitter_percent BETWEEN 0 AND 100),
    CONSTRAINT ck_notification_retry_policy_circuit_threshold CHECK (circuit_failure_threshold > 0),
    CONSTRAINT ck_notification_retry_policy_circuit_seconds CHECK (circuit_open_seconds > 0),
    CONSTRAINT ck_notification_retry_policy_latency CHECK (latency_threshold_milliseconds > 0)
);
CREATE INDEX ix_notification_retry_policy_current
    ON notification_retry_policies(policy_key, revision DESC);

CREATE TABLE notification_dead_letters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id uuid NOT NULL REFERENCES notification_deliveries(id) ON DELETE CASCADE,
    notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    state varchar(24) NOT NULL DEFAULT 'OPEN',
    reason_code varchar(100) NOT NULL,
    error_class varchar(32) NOT NULL,
    attempt_count integer NOT NULL,
    first_failure_at timestamptz NOT NULL DEFAULT now(),
    last_failure_at timestamptz NOT NULL DEFAULT now(),
    source_event_id uuid,
    source_event_type varchar(160),
    revision integer NOT NULL DEFAULT 1,
    resolved_at timestamptz,
    resolution_action varchar(32),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_dead_letter_delivery UNIQUE (delivery_id),
    CONSTRAINT ck_notification_dead_letter_state CHECK (
        state IN ('OPEN', 'IN_REVIEW', 'RETRY_SCHEDULED', 'RESOLVED', 'CANCELLED')
    ),
    CONSTRAINT ck_notification_dead_letter_error_class CHECK (
        error_class IN ('TRANSIENT', 'PERMANENT', 'RATE_LIMITED', 'PROVIDER_OUTAGE',
                        'INVALID_CONTACT', 'BOUNCED', 'UNKNOWN')
    ),
    CONSTRAINT ck_notification_dead_letter_attempts CHECK (attempt_count >= 0),
    CONSTRAINT ck_notification_dead_letter_revision CHECK (revision > 0)
);
CREATE INDEX ix_notification_dead_letter_queue
    ON notification_dead_letters(state, updated_at DESC);

CREATE TABLE notification_repair_decisions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dead_letter_id uuid REFERENCES notification_dead_letters(id) ON DELETE SET NULL,
    delivery_id uuid NOT NULL REFERENCES notification_deliveries(id) ON DELETE CASCADE,
    action varchar(32) NOT NULL,
    reason varchar(1000) NOT NULL,
    expected_delivery_revision integer NOT NULL,
    before_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    after_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    actor_account_type varchar(16),
    actor_account_id uuid,
    idempotency_key varchar(180) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_repair_idempotency UNIQUE (action, idempotency_key),
    CONSTRAINT ck_notification_repair_action CHECK (
        action IN ('RETRY_NOW', 'REROUTE', 'CANCEL', 'RESOLVE_INVALID_CONTACT', 'REPLAY_SOURCE')
    ),
    CONSTRAINT ck_notification_repair_expected_revision CHECK (expected_delivery_revision > 0),
    CONSTRAINT ck_notification_repair_before CHECK (jsonb_typeof(before_snapshot) = 'object'),
    CONSTRAINT ck_notification_repair_after CHECK (jsonb_typeof(after_snapshot) = 'object')
);

ALTER TABLE notification_provider_state
    ADD COLUMN consecutive_failures integer NOT NULL DEFAULT 0,
    ADD COLUMN opened_at timestamptz,
    ADD COLUMN open_until timestamptz,
    ADD COLUMN failure_threshold integer NOT NULL DEFAULT 5,
    ADD COLUMN latency_threshold_milliseconds integer NOT NULL DEFAULT 2000,
    ADD COLUMN last_error_code varchar(100),
    ADD COLUMN revision integer NOT NULL DEFAULT 1,
    ADD CONSTRAINT ck_notification_provider_consecutive_failures CHECK (consecutive_failures >= 0),
    ADD CONSTRAINT ck_notification_provider_failure_threshold CHECK (failure_threshold > 0),
    ADD CONSTRAINT ck_notification_provider_latency_threshold
        CHECK (latency_threshold_milliseconds > 0),
    ADD CONSTRAINT ck_notification_provider_revision CHECK (revision > 0);

INSERT INTO notification_retry_policies(
    policy_key, revision, max_attempts, max_age_seconds,
    initial_delay_seconds, maximum_delay_seconds, jitter_percent,
    circuit_failure_threshold, circuit_open_seconds, latency_threshold_milliseconds
) VALUES (
    'notification.default', 1, 5, 86400, 30, 3600, 20, 5, 30, 2000
)
ON CONFLICT (policy_key, revision) DO NOTHING;

INSERT INTO notification_provider_state(provider, status, circuit_state)
VALUES ('email', 'HEALTHY', 'CLOSED'), ('push', 'HEALTHY', 'CLOSED')
ON CONFLICT (provider) DO NOTHING;
