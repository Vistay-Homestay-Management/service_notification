SET search_path TO app, extensions, public;

ALTER TABLE notifications
    ADD COLUMN source_event_type varchar(160),
    ADD COLUMN source_event_version integer,
    ADD COLUMN source_aggregate_id uuid,
    ADD COLUMN source_aggregate_version bigint,
    ADD COLUMN source_correlation_id varchar(100),
    ADD COLUMN route_key varchar(100),
    ADD COLUMN route_version integer,
    ADD COLUMN category varchar(24),
    ADD COLUMN template_key varchar(100),
    ADD COLUMN template_revision integer,
    ADD COLUMN policy_revision integer,
    ADD COLUMN recipient_resolution_revision bigint,
    ADD COLUMN render_input_fingerprint char(64),
    ADD COLUMN inbox_revision bigint,
    ADD COLUMN notification_revision integer NOT NULL DEFAULT 1;

ALTER TABLE notifications
    ADD CONSTRAINT ck_notification_category
        CHECK (category IS NULL OR category IN ('WELCOME', 'SECURITY', 'BOOKING', 'OPERATIONS', 'FINANCE', 'SYSTEM')),
    ADD CONSTRAINT ck_notification_source_event_version
        CHECK (source_event_version IS NULL OR source_event_version > 0),
    ADD CONSTRAINT ck_notification_source_aggregate_version
        CHECK (source_aggregate_version IS NULL OR source_aggregate_version > 0),
    ADD CONSTRAINT ck_notification_route_version
        CHECK (route_version IS NULL OR route_version > 0),
    ADD CONSTRAINT ck_notification_template_revision
        CHECK (template_revision IS NULL OR template_revision > 0),
    ADD CONSTRAINT ck_notification_policy_revision
        CHECK (policy_revision IS NULL OR policy_revision > 0),
    ADD CONSTRAINT ck_notification_resolution_revision
        CHECK (recipient_resolution_revision IS NULL OR recipient_resolution_revision > 0),
    ADD CONSTRAINT ck_notification_revision
        CHECK (notification_revision > 0),
    ADD CONSTRAINT ck_notification_fingerprint
        CHECK (render_input_fingerprint IS NULL OR render_input_fingerprint ~ '^[0-9a-f]{64}$');

CREATE INDEX ix_notification_route_lineage
    ON notifications(source_event_type, source_event_id, route_key, recipient_account_type, recipient_user_id)
    WHERE source_event_id IS NOT NULL;

ALTER TABLE notification_deliveries
    ADD COLUMN required boolean NOT NULL DEFAULT false,
    ADD COLUMN template_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN policy_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN delivery_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN provider_idempotency_key varchar(200),
    ADD COLUMN error_class varchar(32),
    ADD COLUMN contact_snapshot_revision bigint,
    ADD COLUMN destination_resolution_state varchar(16) NOT NULL DEFAULT 'PENDING';

ALTER TABLE notification_deliveries
    DROP CONSTRAINT ck_notification_destination,
    ADD CONSTRAINT ck_notification_delivery_template_revision CHECK (template_revision > 0),
    ADD CONSTRAINT ck_notification_delivery_policy_revision CHECK (policy_revision > 0),
    ADD CONSTRAINT ck_notification_delivery_revision CHECK (delivery_revision > 0),
    ADD CONSTRAINT ck_notification_delivery_error_class CHECK (
        error_class IS NULL OR error_class IN (
            'TRANSIENT', 'PERMANENT', 'RATE_LIMITED', 'PROVIDER_OUTAGE',
            'INVALID_CONTACT', 'BOUNCED', 'UNKNOWN'
        )
    ),
    ADD CONSTRAINT ck_notification_contact_snapshot_revision CHECK (
        contact_snapshot_revision IS NULL OR contact_snapshot_revision > 0
    ),
    ADD CONSTRAINT ck_notification_destination_resolution CHECK (
        destination_resolution_state IN ('PENDING', 'RESOLVED', 'FAILED')
    ),
    ADD CONSTRAINT ck_notification_destination CHECK (
        channel = 'IN_APP'
        OR (destination_lookup_hash IS NOT NULL AND destination_ciphertext IS NOT NULL)
        OR (status = 'PENDING' AND destination_resolution_state = 'PENDING')
    );

CREATE UNIQUE INDEX ux_notification_provider_idempotency
    ON notification_deliveries(provider_idempotency_key)
    WHERE provider_idempotency_key IS NOT NULL;

CREATE TABLE notification_intake (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_name varchar(120) NOT NULL,
    event_id uuid NOT NULL,
    event_type varchar(160) NOT NULL,
    event_version integer NOT NULL,
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL,
    route_key varchar(100) NOT NULL,
    route_version integer NOT NULL,
    recipient_account_type varchar(16),
    recipient_user_id uuid,
    recipient_resolution_revision bigint,
    payload_fingerprint char(64) NOT NULL,
    deduplication_key char(64) NOT NULL,
    business_deduplication_key char(64) NOT NULL,
    state varchar(24) NOT NULL DEFAULT 'RECEIVED',
    notification_id uuid REFERENCES notifications(id) ON DELETE SET NULL,
    error_code varchar(100),
    safe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    received_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT ck_notification_intake_event_version CHECK (event_version > 0),
    CONSTRAINT ck_notification_intake_aggregate_version CHECK (aggregate_version > 0),
    CONSTRAINT ck_notification_intake_route_version CHECK (route_version > 0),
    CONSTRAINT ck_notification_intake_recipient_type CHECK (
        recipient_account_type IS NULL
        OR recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')
    ),
    CONSTRAINT ck_notification_intake_resolution_revision CHECK (
        recipient_resolution_revision IS NULL OR recipient_resolution_revision > 0
    ),
    CONSTRAINT ck_notification_intake_fingerprint CHECK (payload_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_notification_intake_dedup_key CHECK (deduplication_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_notification_intake_business_key CHECK (business_deduplication_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_notification_intake_state CHECK (
        state IN ('RECEIVED', 'VALIDATED', 'RESOLVED', 'CREATED', 'IGNORED',
                  'QUARANTINED', 'FAILED_REPAIRABLE')
    ),
    CONSTRAINT ck_notification_intake_safe_metadata CHECK (jsonb_typeof(safe_metadata) = 'object'),
    CONSTRAINT ck_notification_intake_completed CHECK (
        (state IN ('CREATED', 'IGNORED', 'QUARANTINED', 'FAILED_REPAIRABLE') AND completed_at IS NOT NULL)
        OR (state IN ('RECEIVED', 'VALIDATED', 'RESOLVED') AND completed_at IS NULL)
    ),
    CONSTRAINT uq_notification_intake_event_route UNIQUE (consumer_name, event_id, route_key),
    CONSTRAINT uq_notification_intake_dedup UNIQUE (deduplication_key),
    CONSTRAINT uq_notification_intake_business UNIQUE (business_deduplication_key)
);
CREATE INDEX ix_notification_intake_state ON notification_intake(state, updated_at);
CREATE INDEX ix_notification_intake_event ON notification_intake(event_type, event_id);

CREATE TABLE notification_recipient_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    profile_id uuid,
    authorization_version bigint NOT NULL,
    resolution_revision bigint NOT NULL,
    contact_snapshot_revision bigint,
    snapshot_fingerprint char(64) NOT NULL,
    resolution_source varchar(40) NOT NULL DEFAULT 'USER_AUTHORIZATION',
    resolved_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_snapshot_recipient_type CHECK (
        recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')
    ),
    CONSTRAINT ck_notification_snapshot_authorization_version CHECK (authorization_version > 0),
    CONSTRAINT ck_notification_snapshot_resolution_revision CHECK (resolution_revision > 0),
    CONSTRAINT ck_notification_snapshot_contact_revision CHECK (
        contact_snapshot_revision IS NULL OR contact_snapshot_revision > 0
    ),
    CONSTRAINT ck_notification_snapshot_fingerprint CHECK (snapshot_fingerprint ~ '^[0-9a-f]{64}$'),
    UNIQUE (notification_id),
    UNIQUE (notification_id, recipient_account_type, recipient_user_id)
);

CREATE TABLE notification_intake_quarantine (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    intake_id uuid REFERENCES notification_intake(id) ON DELETE SET NULL,
    event_id uuid,
    event_type varchar(160),
    event_version integer,
    aggregate_id uuid,
    aggregate_version bigint,
    route_key varchar(100),
    route_version integer,
    payload_fingerprint char(64),
    error_code varchar(100) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'OPEN',
    safe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempts integer NOT NULL DEFAULT 0,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CONSTRAINT ck_notification_quarantine_status CHECK (status IN ('OPEN', 'RESOLVED', 'IGNORED')),
    CONSTRAINT ck_notification_quarantine_attempts CHECK (attempts >= 0),
    CONSTRAINT ck_notification_quarantine_metadata CHECK (jsonb_typeof(safe_metadata) = 'object'),
    CONSTRAINT ck_notification_quarantine_fingerprint CHECK (
        payload_fingerprint IS NULL OR payload_fingerprint ~ '^[0-9a-f]{64}$'
    )
);
CREATE INDEX ix_notification_quarantine_open ON notification_intake_quarantine(status, last_seen_at DESC);
CREATE INDEX ix_notification_quarantine_event ON notification_intake_quarantine(event_id, error_code);
