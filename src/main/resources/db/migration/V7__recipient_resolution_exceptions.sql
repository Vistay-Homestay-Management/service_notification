SET search_path TO app, extensions, public;

-- V7 keeps recipient policy evidence separate from canonical User identity and
-- from the immutable notification/delivery history.
CREATE TABLE notification_recipient_rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_key varchar(160) NOT NULL,
    route_key varchar(100) NOT NULL,
    route_version integer NOT NULL,
    recipient_kind varchar(32) NOT NULL,
    revision integer NOT NULL DEFAULT 1,
    required_channels jsonb NOT NULL DEFAULT '[]'::jsonb,
    optional_channels jsonb NOT NULL DEFAULT '[]'::jsonb,
    source_fields jsonb NOT NULL DEFAULT '{}'::jsonb,
    active boolean NOT NULL DEFAULT true,
    immutable boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_recipient_rule UNIQUE (route_key, route_version, recipient_kind),
    CONSTRAINT ck_notification_recipient_rule_version CHECK (route_version > 0 AND revision > 0),
    CONSTRAINT ck_notification_recipient_rule_kind CHECK (
        recipient_kind IN (
            'DIRECT_ACCOUNT', 'ORGANIZATION_ROLE', 'BRANCH_ROLE',
            'BOOKING_PARTY', 'EXPLICIT_SUBSCRIPTION'
        )
    ),
    CONSTRAINT ck_notification_recipient_rule_required CHECK (jsonb_typeof(required_channels) = 'array'),
    CONSTRAINT ck_notification_recipient_rule_optional CHECK (jsonb_typeof(optional_channels) = 'array'),
    CONSTRAINT ck_notification_recipient_rule_fields CHECK (jsonb_typeof(source_fields) = 'object')
);
CREATE INDEX ix_notification_recipient_rule_active
    ON notification_recipient_rules(route_key, route_version, active);

CREATE TABLE notification_recipient_resolutions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    intake_id uuid REFERENCES notification_intake(id) ON DELETE SET NULL,
    notification_id uuid REFERENCES notifications(id) ON DELETE SET NULL,
    event_id uuid,
    event_type varchar(160),
    route_key varchar(100) NOT NULL,
    route_version integer NOT NULL,
    rule_id uuid NOT NULL REFERENCES notification_recipient_rules(id),
    resolution_revision bigint NOT NULL DEFAULT 1,
    recipient_account_type varchar(16),
    recipient_user_id uuid,
    recipient_kind varchar(32) NOT NULL,
    source_aggregate_version bigint,
    contact_snapshot_revision bigint,
    state varchar(32) NOT NULL DEFAULT 'PENDING',
    parent_resolution_id uuid REFERENCES notification_recipient_resolutions(id),
    purpose varchar(80) NOT NULL DEFAULT 'NOTIFICATION_DELIVERY',
    field_group varchar(40) NOT NULL DEFAULT 'NOTIFICATION_SUMMARY',
    safe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    correlation_id varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_recipient_resolution_route CHECK (route_version > 0),
    CONSTRAINT ck_notification_recipient_resolution_revision CHECK (resolution_revision > 0),
    CONSTRAINT ck_notification_recipient_resolution_account CHECK (
        (recipient_account_type IS NULL AND recipient_user_id IS NULL)
        OR (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST') AND recipient_user_id IS NOT NULL)
    ),
    CONSTRAINT ck_notification_recipient_resolution_kind CHECK (
        recipient_kind IN (
            'DIRECT_ACCOUNT', 'ORGANIZATION_ROLE', 'BRANCH_ROLE',
            'BOOKING_PARTY', 'EXPLICIT_SUBSCRIPTION'
        )
    ),
    CONSTRAINT ck_notification_recipient_resolution_state CHECK (
        state IN ('PENDING', 'RESOLVED', 'PARTIALLY_RESOLVED', 'NO_RECIPIENT',
                  'INVALID_CONTACT', 'FAILED_DEPENDENCY')
    ),
    CONSTRAINT ck_notification_recipient_resolution_metadata CHECK (jsonb_typeof(safe_metadata) = 'object'),
    CONSTRAINT ck_notification_recipient_resolution_field_group CHECK (
        field_group IN ('NOTIFICATION_SUMMARY', 'DESTINATION', 'TRUST_DETAIL', 'INTERNAL_AUDIT')
    ),
    CONSTRAINT uq_notification_recipient_resolution_revision UNIQUE (
        event_id, route_key, route_version, recipient_account_type,
        recipient_user_id, resolution_revision
    )
);
CREATE INDEX ix_notification_recipient_resolution_event
    ON notification_recipient_resolutions(event_id, route_key, created_at DESC);
CREATE INDEX ix_notification_recipient_resolution_notification
    ON notification_recipient_resolutions(notification_id, created_at DESC);
CREATE INDEX ix_notification_recipient_resolution_state
    ON notification_recipient_resolutions(state, updated_at DESC);

CREATE TABLE notification_recipient_candidates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    resolution_id uuid NOT NULL REFERENCES notification_recipient_resolutions(id) ON DELETE CASCADE,
    candidate_key varchar(180) NOT NULL,
    recipient_account_type varchar(16),
    recipient_user_id uuid,
    recipient_kind varchar(32) NOT NULL,
    partition_type varchar(32),
    partition_id uuid,
    resolution_source varchar(60) NOT NULL,
    state varchar(24) NOT NULL DEFAULT 'PENDING',
    reason_code varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_recipient_candidate UNIQUE (resolution_id, candidate_key),
    CONSTRAINT ck_notification_recipient_candidate_account CHECK (
        (recipient_account_type IS NULL AND recipient_user_id IS NULL)
        OR (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST') AND recipient_user_id IS NOT NULL)
    ),
    CONSTRAINT ck_notification_recipient_candidate_kind CHECK (
        recipient_kind IN (
            'DIRECT_ACCOUNT', 'ORGANIZATION_ROLE', 'BRANCH_ROLE',
            'BOOKING_PARTY', 'EXPLICIT_SUBSCRIPTION'
        )
    ),
    CONSTRAINT ck_notification_recipient_candidate_state CHECK (
        state IN ('PENDING', 'ELIGIBLE', 'REJECTED', 'SELECTED')
    )
);
CREATE INDEX ix_notification_recipient_candidate_state
    ON notification_recipient_candidates(state, created_at DESC);

CREATE TABLE notification_contact_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    revision bigint NOT NULL,
    purpose varchar(80) NOT NULL,
    field_group varchar(40) NOT NULL DEFAULT 'DESTINATION',
    destination_masked varchar(254),
    destination_ciphertext bytea,
    verification_status varchar(16) NOT NULL,
    snapshot_fingerprint char(64) NOT NULL,
    source_reference varchar(180),
    immutable boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_contact_snapshot UNIQUE (
        recipient_account_type, recipient_user_id, revision, purpose
    ),
    CONSTRAINT ck_notification_contact_snapshot_account CHECK (
        recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')
    ),
    CONSTRAINT ck_notification_contact_snapshot_revision CHECK (revision > 0),
    CONSTRAINT ck_notification_contact_snapshot_status CHECK (
        verification_status IN ('VERIFIED', 'UNVERIFIED', 'INVALID', 'MISSING')
    ),
    CONSTRAINT ck_notification_contact_snapshot_fingerprint CHECK (
        snapshot_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_notification_contact_snapshot_field_group CHECK (
        field_group IN ('DESTINATION', 'TRUST_DETAIL', 'INTERNAL_AUDIT')
    )
);
CREATE INDEX ix_notification_contact_snapshot_recipient
    ON notification_contact_snapshots(recipient_account_type, recipient_user_id, revision DESC);

CREATE TABLE notification_recipient_decisions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    resolution_id uuid NOT NULL REFERENCES notification_recipient_resolutions(id) ON DELETE CASCADE,
    candidate_id uuid REFERENCES notification_recipient_candidates(id) ON DELETE SET NULL,
    channel varchar(16) NOT NULL,
    decision varchar(16) NOT NULL,
    reason_code varchar(100) NOT NULL,
    purpose varchar(80) NOT NULL,
    field_group varchar(40) NOT NULL DEFAULT 'NOTIFICATION_SUMMARY',
    destination_masked varchar(254),
    contact_snapshot_revision bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_recipient_decision UNIQUE (resolution_id, candidate_id, channel),
    CONSTRAINT ck_notification_recipient_decision_channel CHECK (channel IN ('IN_APP', 'EMAIL', 'PUSH')),
    CONSTRAINT ck_notification_recipient_decision_value CHECK (decision IN ('ALLOW', 'DENY', 'DEFER')),
    CONSTRAINT ck_notification_recipient_decision_field_group CHECK (
        field_group IN ('NOTIFICATION_SUMMARY', 'DESTINATION', 'TRUST_DETAIL', 'INTERNAL_AUDIT')
    ),
    CONSTRAINT ck_notification_recipient_decision_revision CHECK (
        contact_snapshot_revision IS NULL OR contact_snapshot_revision > 0
    )
);
CREATE INDEX ix_notification_recipient_decision_reason
    ON notification_recipient_decisions(reason_code, created_at DESC);

ALTER TABLE notifications
    ADD COLUMN recipient_resolution_id uuid REFERENCES notification_recipient_resolutions(id);

ALTER TABLE notification_deliveries
    ADD COLUMN recipient_resolution_id uuid REFERENCES notification_recipient_resolutions(id),
    ADD COLUMN contact_snapshot_id uuid REFERENCES notification_contact_snapshots(id);

CREATE TABLE notification_recipient_exceptions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    exception_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    resolution_id uuid NOT NULL REFERENCES notification_recipient_resolutions(id) ON DELETE CASCADE,
    notification_id uuid REFERENCES notifications(id) ON DELETE SET NULL,
    recipient_account_id uuid,
    recipient_kind varchar(32) NOT NULL,
    exception_type varchar(32) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'OPEN',
    reason_code varchar(100) NOT NULL,
    resolution_action varchar(32),
    destination_masked varchar(254),
    contact_snapshot_revision bigint,
    revision integer NOT NULL DEFAULT 1,
    correlation_id varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CONSTRAINT ck_notification_recipient_exception_kind CHECK (
        recipient_kind IN (
            'DIRECT_ACCOUNT', 'ORGANIZATION_ROLE', 'BRANCH_ROLE',
            'BOOKING_PARTY', 'EXPLICIT_SUBSCRIPTION'
        )
    ),
    CONSTRAINT ck_notification_recipient_exception_type CHECK (
        exception_type IN (
            'NO_RECIPIENT', 'AMBIGUOUS_RECIPIENT', 'CROSS_PARTITION',
            'MISSING_CONTACT', 'INVALID_CONTACT', 'UNVERIFIED_CONTACT',
            'OPTIONAL_SUPPRESSED', 'DEPENDENCY_UNAVAILABLE'
        )
    ),
    CONSTRAINT ck_notification_recipient_exception_status CHECK (
        status IN ('OPEN', 'IN_REVIEW', 'RESOLVED', 'IGNORED', 'EXPIRED')
    ),
    CONSTRAINT ck_notification_recipient_exception_action CHECK (
        resolution_action IS NULL OR resolution_action IN (
            'RE_RESOLVE', 'SUPPRESS', 'ROUTE_ALTERNATE', 'CANCEL'
        )
    ),
    CONSTRAINT ck_notification_recipient_exception_revision CHECK (revision > 0),
    CONSTRAINT ck_notification_recipient_exception_contact_revision CHECK (
        contact_snapshot_revision IS NULL OR contact_snapshot_revision > 0
    )
);
CREATE INDEX ix_notification_recipient_exception_queue
    ON notification_recipient_exceptions(status, exception_sequence);
CREATE INDEX ix_notification_recipient_exception_notification
    ON notification_recipient_exceptions(notification_id, created_at DESC);
CREATE INDEX ix_notification_recipient_exception_recipient
    ON notification_recipient_exceptions(recipient_account_id, status, created_at DESC);
CREATE UNIQUE INDEX ux_notification_recipient_exception_open
    ON notification_recipient_exceptions(resolution_id, exception_type)
    WHERE status IN ('OPEN', 'IN_REVIEW');

CREATE TABLE notification_recipient_resolution_history (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    exception_id uuid REFERENCES notification_recipient_exceptions(id) ON DELETE SET NULL,
    resolution_id uuid NOT NULL REFERENCES notification_recipient_resolutions(id) ON DELETE CASCADE,
    from_status varchar(16),
    to_status varchar(16) NOT NULL,
    action varchar(32) NOT NULL,
    reason varchar(1000) NOT NULL,
    expected_revision integer,
    actor_account_type varchar(16),
    actor_account_id uuid,
    idempotency_key varchar(180),
    safe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_recipient_history_action CHECK (
        action IN ('RE_RESOLVE', 'SUPPRESS', 'ROUTE_ALTERNATE', 'CANCEL', 'RECORD_FAILURE')
    ),
    CONSTRAINT ck_notification_recipient_history_status CHECK (
        to_status IN ('OPEN', 'IN_REVIEW', 'RESOLVED', 'IGNORED', 'EXPIRED')
    ),
    CONSTRAINT ck_notification_recipient_history_metadata CHECK (jsonb_typeof(safe_metadata) = 'object'),
    CONSTRAINT uq_notification_recipient_history_idempotency UNIQUE (action, idempotency_key)
);
CREATE INDEX ix_notification_recipient_history_exception
    ON notification_recipient_resolution_history(exception_id, created_at DESC);
