SET search_path TO app, extensions, public;

-- V4 is additive. Existing V2/V3 rows remain readable and retain their
-- original revision numbers; newly published revisions are immutable.
ALTER TABLE notification_templates
    ADD COLUMN status varchar(16) NOT NULL DEFAULT 'DRAFT',
    ADD COLUMN current_revision integer,
    ADD COLUMN policy_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN published_at timestamptz,
    ADD COLUMN retired_at timestamptz,
    ADD CONSTRAINT ck_notification_template_status
        CHECK (status IN ('DRAFT', 'PUBLISHED', 'RETIRED')),
    ADD CONSTRAINT ck_notification_template_current_revision
        CHECK (current_revision IS NULL OR current_revision > 0),
    ADD CONSTRAINT ck_notification_template_policy_revision
        CHECK (policy_revision > 0);

ALTER TABLE notification_template_revisions
    ADD COLUMN status varchar(16) NOT NULL DEFAULT 'DRAFT',
    ADD COLUMN required_variables jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN variables_schema jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN policy_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN published_at timestamptz,
    ADD COLUMN retired_at timestamptz,
    ADD COLUMN immutable boolean NOT NULL DEFAULT false,
    ADD COLUMN revision_fingerprint char(64),
    ADD CONSTRAINT ck_notification_template_revision_status
        CHECK (status IN ('DRAFT', 'PUBLISHED', 'RETIRED')),
    ADD CONSTRAINT ck_notification_template_revision_required_variables
        CHECK (jsonb_typeof(required_variables) = 'array'),
    ADD CONSTRAINT ck_notification_template_revision_variables_schema
        CHECK (jsonb_typeof(variables_schema) = 'object'),
    ADD CONSTRAINT ck_notification_template_revision_policy
        CHECK (policy_revision > 0),
    ADD CONSTRAINT ck_notification_template_revision_fingerprint
        CHECK (revision_fingerprint IS NULL OR revision_fingerprint ~ '^[0-9a-f]{64}$');

UPDATE notification_template_revisions r
SET required_variables = r.template_variables,
    status = CASE WHEN t.is_active AND r.version_number = (
        SELECT max(latest.version_number)
        FROM notification_template_revisions latest
        WHERE latest.template_id = r.template_id
    ) THEN 'PUBLISHED' ELSE 'RETIRED' END,
    immutable = true,
    published_at = CASE WHEN t.is_active AND r.version_number = (
        SELECT max(latest.version_number)
        FROM notification_template_revisions latest
        WHERE latest.template_id = r.template_id
    ) THEN r.created_at ELSE NULL END
FROM notification_templates t
WHERE t.id = r.template_id;

UPDATE notification_templates t
SET status = CASE WHEN t.is_active THEN 'PUBLISHED' ELSE 'RETIRED' END,
    current_revision = (
        SELECT max(r.version_number)
        FROM notification_template_revisions r
        WHERE r.template_id = t.id
    ),
    published_at = CASE WHEN t.is_active THEN t.created_at ELSE NULL END;

CREATE UNIQUE INDEX ux_notification_template_published
    ON notification_template_revisions(template_id)
    WHERE status = 'PUBLISHED';
CREATE INDEX ix_notification_template_catalog
    ON notification_templates(code, channel, language_code, status);
CREATE INDEX ix_notification_template_revision_catalog
    ON notification_template_revisions(template_id, status, version_number DESC);

ALTER TABLE notification_preferences
    ADD COLUMN revision integer NOT NULL DEFAULT 1,
    ADD COLUMN device_reference varchar(180),
    ADD COLUMN feature_policy_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD CONSTRAINT ck_notification_preference_revision CHECK (revision > 0),
    ADD CONSTRAINT ck_notification_preference_device_reference
        CHECK (device_reference IS NULL OR device_reference ~ '^[A-Za-z0-9._:-]{1,180}$'),
    ADD CONSTRAINT ck_notification_preference_feature_policy
        CHECK (jsonb_typeof(feature_policy_snapshot) = 'object');
CREATE INDEX ix_notification_preference_recipient_revision
    ON notification_preferences(recipient_account_type, recipient_user_id, revision DESC);

ALTER TABLE notification_deliveries
    ADD COLUMN template_key varchar(100),
    ADD COLUMN preference_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN feature_policy_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN device_reference varchar(180),
    ADD COLUMN quiet_hours_deferred_until timestamptz,
    ADD CONSTRAINT ck_notification_delivery_preference_revision
        CHECK (preference_revision > 0),
    ADD CONSTRAINT ck_notification_delivery_feature_policy
        CHECK (jsonb_typeof(feature_policy_snapshot) = 'object'),
    ADD CONSTRAINT ck_notification_delivery_device_reference
        CHECK (device_reference IS NULL OR device_reference ~ '^[A-Za-z0-9._:-]{1,180}$');
CREATE INDEX ix_notification_delivery_pending_v4
    ON notification_deliveries(status, next_attempt_at, quiet_hours_deferred_until)
    WHERE status IN ('PENDING', 'FAILED');

CREATE TABLE notification_policies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_key varchar(100) NOT NULL,
    revision integer NOT NULL,
    required_channels jsonb NOT NULL,
    optional_channels jsonb NOT NULL DEFAULT '[]'::jsonb,
    push_enabled boolean NOT NULL DEFAULT false,
    allowed_notification_types jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    immutable boolean NOT NULL DEFAULT true,
    CONSTRAINT uq_notification_policy_version UNIQUE (policy_key, revision),
    CONSTRAINT ck_notification_policy_revision CHECK (revision > 0),
    CONSTRAINT ck_notification_policy_required_channels
        CHECK (jsonb_typeof(required_channels) = 'array'),
    CONSTRAINT ck_notification_policy_optional_channels
        CHECK (jsonb_typeof(optional_channels) = 'array'),
    CONSTRAINT ck_notification_policy_allowed_types
        CHECK (jsonb_typeof(allowed_notification_types) = 'array')
);
CREATE INDEX ix_notification_policy_current
    ON notification_policies(policy_key, revision DESC);

CREATE TABLE notification_device_references (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    device_reference varchar(180) NOT NULL,
    provider varchar(80) NOT NULL,
    active boolean NOT NULL DEFAULT true,
    policy_revision integer NOT NULL DEFAULT 1,
    last_seen_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_device_account_type
        CHECK (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_notification_device_reference
        CHECK (device_reference ~ '^[A-Za-z0-9._:-]{1,180}$'),
    CONSTRAINT ck_notification_device_policy_revision CHECK (policy_revision > 0),
    UNIQUE (recipient_account_type, recipient_user_id, device_reference, provider)
);
CREATE INDEX ix_notification_device_active
    ON notification_device_references(recipient_account_type, recipient_user_id, active);

CREATE TABLE notification_command_idempotency (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    operation varchar(100) NOT NULL,
    idempotency_key varchar(180) NOT NULL,
    actor_account_type varchar(16),
    actor_user_id uuid,
    request_fingerprint char(64) NOT NULL,
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_command_idempotency UNIQUE (operation, idempotency_key),
    CONSTRAINT ck_notification_command_fingerprint CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_notification_command_result CHECK (jsonb_typeof(result) = 'object')
);

CREATE TABLE notification_provider_state (
    provider varchar(80) PRIMARY KEY,
    status varchar(16) NOT NULL DEFAULT 'HEALTHY',
    circuit_state varchar(16) NOT NULL DEFAULT 'CLOSED',
    failure_count integer NOT NULL DEFAULT 0,
    success_count integer NOT NULL DEFAULT 0,
    latency_milliseconds integer NOT NULL DEFAULT 0,
    retry_after_seconds integer,
    checked_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_provider_status
        CHECK (status IN ('HEALTHY', 'DEGRADED', 'UNAVAILABLE')),
    CONSTRAINT ck_notification_provider_circuit
        CHECK (circuit_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    CONSTRAINT ck_notification_provider_counts CHECK (failure_count >= 0 AND success_count >= 0),
    CONSTRAINT ck_notification_provider_latency CHECK (latency_milliseconds >= 0),
    CONSTRAINT ck_notification_provider_retry_after
        CHECK (retry_after_seconds IS NULL OR retry_after_seconds >= 0)
);

INSERT INTO notification_policies(
    policy_key, revision, required_channels, optional_channels,
    push_enabled, allowed_notification_types
) VALUES (
    'notification.default', 1, '["IN_APP", "EMAIL"]'::jsonb,
    '["PUSH"]'::jsonb, true, '[]'::jsonb
)
ON CONFLICT (policy_key, revision) DO NOTHING;
