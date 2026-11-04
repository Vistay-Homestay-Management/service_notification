# ViStay Notification Service

Notification inbox, delivery, templates, and SSE streams.

This repository is part of the ViStay Homestay Management platform. Epic 1 establishes a buildable, testable application shell and platform integrations; domain workflows are delivered by their owning epics.

## Notification channel delivery

V4 keeps template revisions immutable after publication. Recipient preferences
use optimistic revision checks and idempotency keys; `IN_APP` and `EMAIL` are
mandatory, while `PUSH` additionally requires the global feature flag, route
permission, an active opaque device reference, and a valid optional preference.
Provider adapters receive only masked destinations and encrypted snapshots.

The delivery polling worker is disabled by default. Enable it only when the
approved email/push provider adapters are configured:

```text
NOTIFICATION_DELIVERY_WORKER_ENABLED=true
NOTIFICATION_PROVIDER_TIMEOUT=2s
```

For a safe repair check, inspect pending/failed rows without selecting raw
destination columns:

```sql
SELECT id, channel, status, attempt_count, maximum_attempts,
       next_attempt_at, last_error_code, error_class
FROM app.notification_deliveries
WHERE status IN ('PENDING', 'FAILED')
ORDER BY updated_at;
```

Retry only through the versioned notification operation after confirming the
template, preference, contact-snapshot, and provider dependency state.

V5 adds a `SKIP LOCKED` lease/heartbeat, exponential bounded backoff with
jitter, provider circuit state, and an immutable dead-letter/repair history.
Expired leases are returned to the queue; a dead-lettered delivery is not
retried until an authorized repair command supplies a reason, expected
delivery revision, and idempotency key.

V6 makes the recipient inbox durable. Inbox pages and unread counts are scoped
 by the authenticated recipient and expose an exclusive cursor with retained
 watermark metadata. Mark-read and mark-all-read commands use optimistic
 notification revisions, durable read receipts, and idempotency keys; each
 successful transition is auditable and published through the outbox.

The notification stream is bounded by duration and polling interval, records
connection/heartbeat metadata, limits concurrent connections per recipient,
and advertises at-least-once delivery so clients can resume and deduplicate by
notification id plus inbox revision.
