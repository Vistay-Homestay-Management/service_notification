SET search_path TO app, extensions, public;

CREATE TABLE notification_templates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code varchar(100) NOT NULL,
    channel varchar(16) NOT NULL,
    language_code varchar(10) NOT NULL DEFAULT 'vi',
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_template_channel CHECK (channel IN ('IN_APP', 'EMAIL', 'SMS', 'PUSH', 'ZALO')),
    UNIQUE (code, channel, language_code)
);

CREATE TABLE notification_template_revisions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    template_id uuid NOT NULL REFERENCES notification_templates(id) ON DELETE CASCADE,
    version_number integer NOT NULL,
    subject_template varchar(500),
    body_template text NOT NULL,
    template_variables jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_by_admin_user_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_template_revision_version CHECK (version_number > 0),
    CONSTRAINT ck_template_revision_variables CHECK (jsonb_typeof(template_variables) = 'array'),
    UNIQUE (template_id, version_number)
);

CREATE TABLE notifications (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    notification_type varchar(100) NOT NULL,
    title varchar(500) NOT NULL,
    body text NOT NULL,
    priority varchar(16) NOT NULL DEFAULT 'NORMAL',
    source_service varchar(80) NOT NULL,
    source_event_id uuid,
    reference_type varchar(60),
    reference_id uuid,
    action_url varchar(1000),
    data jsonb NOT NULL DEFAULT '{}'::jsonb,
    status varchar(24) NOT NULL DEFAULT 'PENDING',
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    read_at timestamptz,
    cancelled_at timestamptz,
    cancellation_reason varchar(500),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_recipient_type CHECK (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_notification_priority CHECK (priority IN ('LOW', 'NORMAL', 'HIGH', 'URGENT')),
    CONSTRAINT ck_notification_reference CHECK ((reference_type IS NULL) = (reference_id IS NULL)),
    CONSTRAINT ck_notification_data CHECK (jsonb_typeof(data) = 'object'),
    CONSTRAINT ck_notification_status CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'PARTIALLY_SENT', 'FAILED', 'CANCELLED')),
    CONSTRAINT ck_notification_expiry CHECK (expires_at IS NULL OR expires_at > created_at),
    CONSTRAINT ck_notification_cancellation CHECK ((status = 'CANCELLED' AND cancelled_at IS NOT NULL) OR status <> 'CANCELLED')
);
CREATE INDEX ix_notification_inbox ON notifications(recipient_account_type, recipient_user_id, read_at, created_at DESC);
CREATE INDEX ix_notification_sse_cursor ON notifications(recipient_account_type, recipient_user_id, event_sequence);
CREATE INDEX ix_notification_queue ON notifications(status, scheduled_at) WHERE status IN ('PENDING', 'PROCESSING');
CREATE UNIQUE INDEX ux_notification_source_dedup
    ON notifications(source_service, source_event_id, notification_type, recipient_account_type, recipient_user_id)
    WHERE source_event_id IS NOT NULL;

CREATE TABLE notification_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    channel varchar(16) NOT NULL,
    template_id uuid REFERENCES notification_templates(id) ON DELETE SET NULL,
    template_revision_id uuid REFERENCES notification_template_revisions(id) ON DELETE SET NULL,
    provider varchar(80),
    destination_masked varchar(254),
    destination_lookup_hash char(64),
    destination_ciphertext bytea,
    status varchar(24) NOT NULL DEFAULT 'PENDING',
    attempt_count integer NOT NULL DEFAULT 0,
    maximum_attempts integer NOT NULL DEFAULT 5,
    next_attempt_at timestamptz,
    provider_message_id varchar(180),
    accepted_at timestamptz,
    sent_at timestamptz,
    delivered_at timestamptz,
    read_at timestamptz,
    failed_at timestamptz,
    last_error_code varchar(100),
    last_error_message varchar(1000),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_delivery_channel CHECK (channel IN ('IN_APP', 'EMAIL', 'SMS', 'PUSH', 'ZALO')),
    CONSTRAINT ck_notification_delivery_status CHECK (status IN ('PENDING', 'SENDING', 'ACCEPTED', 'SENT', 'DELIVERED', 'READ', 'FAILED', 'BOUNCED', 'CANCELLED')),
    CONSTRAINT ck_notification_delivery_attempts CHECK (attempt_count >= 0 AND maximum_attempts > 0 AND attempt_count <= maximum_attempts),
    CONSTRAINT ck_notification_destination CHECK (
        channel = 'IN_APP'
        OR (destination_lookup_hash IS NOT NULL AND destination_ciphertext IS NOT NULL)
    ),
    UNIQUE (notification_id, channel)
);
CREATE INDEX ix_notification_delivery_retry ON notification_deliveries(status, next_attempt_at) WHERE status IN ('PENDING', 'FAILED');
CREATE UNIQUE INDEX ux_notification_provider_message
    ON notification_deliveries(provider, provider_message_id)
    WHERE provider IS NOT NULL AND provider_message_id IS NOT NULL;

CREATE TABLE delivery_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id uuid NOT NULL REFERENCES notification_deliveries(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL,
    result varchar(24) NOT NULL,
    provider_response_code varchar(100),
    provider_request_id varchar(180),
    error_code varchar(100),
    error_message varchar(1000),
    attempted_at timestamptz NOT NULL DEFAULT now(),
    duration_ms integer,
    CONSTRAINT ck_delivery_attempt_number CHECK (attempt_number > 0),
    CONSTRAINT ck_delivery_attempt_result CHECK (result IN ('ACCEPTED', 'SENT', 'FAILED', 'TIMEOUT', 'RATE_LIMITED', 'BOUNCED')),
    CONSTRAINT ck_delivery_attempt_duration CHECK (duration_ms IS NULL OR duration_ms >= 0),
    UNIQUE (delivery_id, attempt_number)
);

CREATE TABLE notification_preferences (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    notification_type varchar(100) NOT NULL DEFAULT '*',
    channel varchar(16) NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    quiet_hours_start time,
    quiet_hours_end time,
    time_zone_id varchar(80) NOT NULL DEFAULT 'Asia/Ho_Chi_Minh',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_preference_recipient CHECK (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_notification_preference_channel CHECK (channel IN ('IN_APP', 'EMAIL', 'SMS', 'PUSH', 'ZALO')),
    CONSTRAINT ck_notification_quiet_hours CHECK ((quiet_hours_start IS NULL) = (quiet_hours_end IS NULL)),
    UNIQUE (recipient_account_type, recipient_user_id, notification_type, channel)
);

CREATE TABLE processed_events (
    event_id uuid NOT NULL,
    consumer_name varchar(120) NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_name)
);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type varchar(100) NOT NULL,
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version >= 0),
    event_type varchar(160) NOT NULL,
    event_version integer NOT NULL DEFAULT 1 CHECK (event_version > 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    occurred_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    publish_attempts integer NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    last_error text,
    correlation_id varchar(100),
    causation_id varchar(100)
);
CREATE INDEX ix_notification_outbox_pending ON outbox_events(available_at, occurred_at) WHERE published_at IS NULL;

CREATE TABLE audit_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_admin_user_id uuid,
    action varchar(100) NOT NULL,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id uuid NOT NULL,
    changes jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(changes) = 'object'),
    correlation_id varchar(100),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_notification_audit_aggregate ON audit_events(aggregate_type, aggregate_id, created_at DESC);
