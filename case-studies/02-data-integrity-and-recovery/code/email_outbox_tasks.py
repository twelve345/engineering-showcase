"""ARQ cron task for processing the email outbox.

Runs every 10 seconds. Uses a two-phase commit to reduce the window
for duplicate sends on worker crash:

  Phase 1 — Claim: SELECT PENDING rows FOR UPDATE SKIP LOCKED,
            mark PROCESSING, COMMIT. Rows are now visible to other
            workers as claimed.
  Phase 2 — Send: For each claimed row, dispatch the email and
            commit the result (SENT/FAILED) individually. A crash
            after send but before commit leaves the row as PROCESSING
            (not PENDING), so it will not be immediately resent.

Stale PROCESSING rows (worker crashed mid-batch) are reclaimed back
to PENDING by ``reclaim_stale_processing_rows``, which runs on a
separate schedule. This means a crash *after* a successful send but
*before* the SENT commit can still cause a duplicate send once the
row is reclaimed (rare edge case).

Dispatch routing (mapping outbox rows to ``EmailService`` methods)
lives in ``app.services.email_dispatch_service``.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import and_, or_, select, update

from app.models.email_outbox import EmailOutbox, EmailOutboxStatus
from app.services.email_dispatch_service import email_dispatch_service

logger = structlog.get_logger(__name__)

BATCH_SIZE = 50
SEND_DELAY_SECONDS = 0.5  # rate limit compliance
# Worst-case delay for crash recovery: STALE_THRESHOLD_MINUTES + reclaim
# interval (5 min) ≈ 15 minutes before a crashed row is resent.
STALE_THRESHOLD_MINUTES = 10


async def process_email_outbox(ctx: dict[str, Any]) -> dict[str, int]:
    """Pick up pending outbox rows and send them via two-phase commit.

    Phase 1: Claim rows (PENDING → PROCESSING) and commit.
    Phase 2: Send each email and commit the result individually.

    Returns a summary dict for job history logging.
    """
    session_maker = ctx["session_maker"]
    sent = 0
    failed = 0
    abandoned = 0

    # ------------------------------------------------------------------
    # Phase 1 — Claim: mark rows as PROCESSING and commit
    # ------------------------------------------------------------------
    claimed_ids: list[int] = []
    async with session_maker() as db:
        result = await db.execute(
            select(EmailOutbox)
            .where(
                and_(
                    EmailOutbox.status == EmailOutboxStatus.PENDING,
                    EmailOutbox.attempt_count < EmailOutbox.max_attempts,
                )
            )
            .order_by(EmailOutbox.created_at)
            .limit(BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        outbox_rows = list(result.scalars().all())

        if not outbox_rows:
            return {"sent": 0, "failed": 0, "abandoned": 0}

        now = datetime.now(UTC)
        for row in outbox_rows:
            row.status = EmailOutboxStatus.PROCESSING
            row.claimed_at = now
            claimed_ids.append(row.id)

        await db.commit()

    logger.info("Claimed outbox rows for processing", count=len(claimed_ids))

    # ------------------------------------------------------------------
    # Phase 2 — Send: process each row in its own transaction
    # ------------------------------------------------------------------
    for idx, outbox_id in enumerate(claimed_ids):
        async with session_maker() as db:
            result = await db.execute(
                select(EmailOutbox).where(EmailOutbox.id == outbox_id)
            )
            outbox = result.scalar_one_or_none()
            if not outbox or outbox.status != EmailOutboxStatus.PROCESSING:
                continue

            try:
                success = await email_dispatch_service.dispatch(db, outbox)

                if success:
                    outbox.status = EmailOutboxStatus.SENT
                    outbox.sent_at = datetime.now(UTC)
                    outbox.attempt_count += 1
                    sent += 1
                    logger.info(
                        "Outbox email sent",
                        outbox_id=outbox.id,
                        idempotency_key=outbox.idempotency_key,
                        email_type=outbox.email_type,
                    )
                else:
                    outbox.attempt_count += 1
                    outbox.last_error = "EmailService returned False"
                    if outbox.attempt_count >= outbox.max_attempts:
                        outbox.status = EmailOutboxStatus.ABANDONED
                        abandoned += 1
                        await _alert_abandoned(outbox)
                    else:
                        outbox.status = EmailOutboxStatus.PENDING
                        outbox.claimed_at = None
                        failed += 1
                    logger.warning(
                        "Outbox email send returned false",
                        outbox_id=outbox.id,
                        attempt=outbox.attempt_count,
                        max_attempts=outbox.max_attempts,
                    )

            except Exception as e:
                outbox.attempt_count += 1
                outbox.last_error = f"{type(e).__name__}: {e}"
                if outbox.attempt_count >= outbox.max_attempts:
                    outbox.status = EmailOutboxStatus.ABANDONED
                    abandoned += 1
                    await _alert_abandoned(outbox)
                else:
                    outbox.status = EmailOutboxStatus.PENDING
                    outbox.claimed_at = None
                    failed += 1
                logger.error(
                    "Outbox email dispatch error",
                    outbox_id=outbox.id,
                    error=str(e),
                    attempt=outbox.attempt_count,
                    exc_info=True,
                )

            await db.commit()

        # Rate limit between sends (skip after last one)
        if idx < len(claimed_ids) - 1:
            await asyncio.sleep(SEND_DELAY_SECONDS)

    summary = {"sent": sent, "failed": failed, "abandoned": abandoned}
    logger.info("Email outbox processing complete", **summary)
    return summary


async def reclaim_stale_processing_rows(ctx: dict[str, Any]) -> dict[str, int]:
    """Reclaim PROCESSING rows stuck longer than the stale threshold.

    A row stuck in PROCESSING means the worker crashed after claiming
    it but before finishing. Reset it to PENDING so the next run
    picks it up.
    """
    session_maker = ctx["session_maker"]
    cutoff = datetime.now(UTC) - timedelta(minutes=STALE_THRESHOLD_MINUTES)

    async with session_maker() as db:
        result = await db.execute(
            update(EmailOutbox)
            .where(
                and_(
                    EmailOutbox.status == EmailOutboxStatus.PROCESSING,
                    or_(
                        EmailOutbox.claimed_at < cutoff,
                        EmailOutbox.claimed_at.is_(None),
                    ),
                )
            )
            .values(status=EmailOutboxStatus.PENDING, claimed_at=None)
            .returning(EmailOutbox.id)
        )
        reclaimed_ids = list(result.scalars().all())
        await db.commit()

    if reclaimed_ids:
        logger.warning(
            "Reclaimed stale PROCESSING outbox rows",
            count=len(reclaimed_ids),
            ids=reclaimed_ids,
        )

    return {"reclaimed": len(reclaimed_ids)}


async def _alert_abandoned(outbox: EmailOutbox) -> None:
    """Send an alert when an outbox row is abandoned after max attempts."""
    try:
        from app.services.alerting_service import alerting_service

        await alerting_service.send_alert(
            title=f"Email Abandoned: {outbox.email_type}",
            error=outbox.last_error or "Max attempts reached",
            context={
                "outbox_id": str(outbox.id),
                "idempotency_key": outbox.idempotency_key,
                "email_type": outbox.email_type,
                "to_email": outbox.to_email,
                "attempt_count": str(outbox.attempt_count),
            },
        )
    except Exception as e:
        logger.warning(
            "Failed to send abandoned email alert",
            outbox_id=outbox.id,
            error=str(e),
        )
