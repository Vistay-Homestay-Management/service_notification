SET search_path TO app, extensions, public;

CREATE TABLE profile_link_email_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_event_id uuid NOT NULL UNIQUE,
    link_id uuid NOT NULL,
    challenge_id uuid NOT NULL UNIQUE,
    profile_type varchar(16) NOT NULL,
    masked_recipient varchar(254) NOT NULL,
    delivery_ciphertext text NOT NULL,
    delivery_key_version integer NOT NULL CHECK (delivery_key_version > 0),
    status varchar(24) NOT NULL DEFAULT 'PENDING',
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    maximum_attempts integer NOT NULL DEFAULT 5 CHECK (maximum_attempts BETWEEN 1 AND 20),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    provider_message_id varchar(180),
    expires_at timestamptz NOT NULL,
    sent_at timestamptz,
    last_error_code varchar(100),
    last_error_message varchar(500),
    correlation_id varchar(100),
    causation_id varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_profile_link_email_type CHECK (profile_type IN ('CUSTOMER', 'ADMIN', 'HOST')),
    CONSTRAINT ck_profile_link_email_status CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'FAILED', 'EXPIRED')),
    CONSTRAINT ck_profile_link_email_ciphertext CHECK (delivery_ciphertext ~ '^vault:v[0-9]+:'),
    CONSTRAINT ck_profile_link_email_expiry CHECK (expires_at > created_at)
);

CREATE INDEX ix_profile_link_email_pending
    ON profile_link_email_deliveries(next_attempt_at, created_at)
    WHERE status IN ('PENDING', 'FAILED');

COMMENT ON TABLE profile_link_email_deliveries IS
    'Durable profile-link OTP delivery queue. Contains Vault Transit ciphertext only; plaintext email and OTP are forbidden.';
