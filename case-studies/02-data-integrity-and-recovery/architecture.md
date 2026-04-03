# Architecture

- Use a two-phase outbox so claim and send are separate failure domains.
- Reclaim stale work explicitly instead of assuming the worker always completes.
- Treat reconciliation as a safety net for external-system drift, not the primary source of truth.
- Write tests around retries, recovery, and idempotency boundaries.
