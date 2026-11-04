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
