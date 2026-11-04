SET search_path TO app, extensions, public;

ALTER TABLE notifications
    ADD COLUMN read_revision integer NOT NULL DEFAULT 0,
    ADD COLUMN read_correlation_id varchar(100),
    ADD CONSTRAINT ck_notification_read_revision CHECK (read_revision >= 0);

CREATE INDEX ix_notification_unread_cursor
    ON notifications(recipient_account_type, recipient_user_id, read_at, event_sequence)
    WHERE status <> 'CANCELLED';

CREATE TABLE notification_read_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    read_at timestamptz NOT NULL DEFAULT now(),
    revision integer NOT NULL DEFAULT 1,
    idempotency_key varchar(180),
    correlation_id varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_read_receipt UNIQUE (notification_id),
    CONSTRAINT ck_notification_read_receipt_account_type
        CHECK (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_notification_read_receipt_revision CHECK (revision > 0)
);
CREATE INDEX ix_notification_read_receipt_recipient
    ON notification_read_receipts(recipient_account_type, recipient_user_id, read_at DESC);

CREATE TABLE notification_cursor_watermarks (
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    retained_from bigint NOT NULL DEFAULT 0,
    latest_cursor bigint NOT NULL DEFAULT 0,
    revision integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (recipient_account_type, recipient_user_id),
    CONSTRAINT ck_notification_cursor_account_type
        CHECK (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_notification_cursor_retained_from CHECK (retained_from >= 0),
    CONSTRAINT ck_notification_cursor_latest CHECK (latest_cursor >= retained_from),
    CONSTRAINT ck_notification_cursor_revision CHECK (revision > 0)
);

CREATE TABLE notification_stream_connections (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_account_type varchar(16) NOT NULL,
    recipient_user_id uuid NOT NULL,
    after_cursor bigint NOT NULL DEFAULT 0,
    state varchar(16) NOT NULL DEFAULT 'ACTIVE',
    opened_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    close_reason varchar(100),
    emitted_frames integer NOT NULL DEFAULT 0,
    max_duration_seconds integer NOT NULL DEFAULT 1800,
    CONSTRAINT ck_notification_stream_account_type
        CHECK (recipient_account_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_notification_stream_cursor CHECK (after_cursor >= 0),
    CONSTRAINT ck_notification_stream_state CHECK (state IN ('ACTIVE', 'CLOSED', 'EXPIRED')),
    CONSTRAINT ck_notification_stream_frames CHECK (emitted_frames >= 0),
    CONSTRAINT ck_notification_stream_duration CHECK (max_duration_seconds > 0)
);
CREATE INDEX ix_notification_stream_active
    ON notification_stream_connections(recipient_account_type, recipient_user_id, state, opened_at);

INSERT INTO notification_cursor_watermarks(
    recipient_account_type, recipient_user_id, latest_cursor
)
SELECT n.recipient_account_type, n.recipient_user_id, max(n.event_sequence)
FROM notifications n
GROUP BY n.recipient_account_type, n.recipient_user_id
ON CONFLICT (recipient_account_type, recipient_user_id) DO UPDATE
SET latest_cursor = GREATEST(notification_cursor_watermarks.latest_cursor, EXCLUDED.latest_cursor),
    updated_at = now();
