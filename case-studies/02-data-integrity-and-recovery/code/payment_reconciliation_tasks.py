"""
Payment Reconciliation Task - Safety Net for Payment Drift

This task runs periodically (recommended: hourly) to detect and fix
payment state drift between Stripe and our database.

Key scenarios handled:
1. PENDING payments where Stripe shows "succeeded" (webhook missed/failed)
2. PENDING payments where Stripe shows "canceled" or "failed" (cleanup)
3. Long-stuck PENDING payments that need attention

This is the final safety net in the bulletproof payment system:
- Primary: Client confirms payment via /confirm-payment endpoint
- Secondary: Webhook confirms payment via payment_intent.succeeded
- Tertiary: This reconciliation task catches any drift
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import stripe
import structlog
from sqlalchemy import and_, select
from sqlalchemy.orm import load_only

from app.core.utils import utc_now_iso
from app.models.organization import Organization
from app.models.payment import Payment, PaymentMethod, PaymentStatus
from app.services.payment_orchestrator import PaymentOrchestrator

logger = structlog.get_logger(__name__)

# Payments older than this threshold (in minutes) are eligible for reconciliation
RECONCILIATION_THRESHOLD_MINUTES = 30


async def reconcile_payments(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Reconcile payment state between database and Stripe.

    This task:
    1. Finds PENDING payments older than 30 minutes
    2. Checks their actual status on Stripe
    3. If succeeded on Stripe: confirms via PaymentOrchestrator
    4. If failed/canceled on Stripe: updates database status
    5. Logs all findings for monitoring/alerting

    Args:
        ctx: ARQ context with session_maker

    Returns:
        Dict with reconciliation statistics
    """
    logger.info("Starting payment reconciliation task...")

    session_maker = ctx.get("session_maker")

    if not session_maker:
        logger.error("Missing session_maker in context")
        return {"error": "Missing dependencies", "processed": 0}

    stats = {
        "processed": 0,
        "reconciled_succeeded": 0,
        "reconciled_failed": 0,
        "reconciled_canceled": 0,
        "already_correct": 0,
        "errors": 0,
        "timestamp": utc_now_iso(),
    }
    org_results: dict[int, dict[str, Any]] = {}

    # Get all org_ids (exclude encrypted columns to avoid decryption failures)
    async with session_maker() as db:
        try:
            org_result = await db.execute(
                select(Organization).options(
                    load_only(
                        Organization.id,
                        Organization.slug,
                    )
                )
            )
            orgs = list(org_result.scalars().all())

            logger.info(
                "Reconciling payments for organizations",
                org_count=len(orgs),
            )

        except Exception as e:
            logger.error(
                "Error fetching organizations for reconciliation",
                error=str(e),
                exc_info=True,
            )
            return {"error": str(e), "processed": 0}

    # Process each organization separately
    for org in orgs:
        try:
            org_stats = await _reconcile_org_payments(session_maker, org)
        except Exception as e:
            logger.error(
                "Failed to reconcile org, skipping",
                org_id=org.id,
                org_slug=org.slug,
                error=str(e),
                exc_info=True,
            )
            org_stats = {"errors": 1}
        org_results[org.id] = org_stats

        # Aggregate stats
        stats["processed"] += org_stats.get("processed", 0)
        stats["reconciled_succeeded"] += org_stats.get("reconciled_succeeded", 0)
        stats["reconciled_failed"] += org_stats.get("reconciled_failed", 0)
        stats["reconciled_canceled"] += org_stats.get("reconciled_canceled", 0)
        stats["already_correct"] += org_stats.get("already_correct", 0)
        stats["errors"] += org_stats.get("errors", 0)

    # Log summary with alert if any drift was found
    total_drift = (
        stats["reconciled_succeeded"]
        + stats["reconciled_failed"]
        + stats["reconciled_canceled"]
    )

    if total_drift > 0:
        logger.warning(
            "Payment drift detected and corrected",
            total_drift=total_drift,
            **stats,
        )
        # Alert on drift detection
        try:
            from app.services.alerting_service import alerting_service

            await alerting_service.send_alert(
                title="Payment Drift Detected",
                error=f"{total_drift} payment(s) had state drift between Stripe and DB",
                context={
                    "total_drift": total_drift,
                    "reconciled_succeeded": stats["reconciled_succeeded"],
                    "reconciled_failed": stats["reconciled_failed"],
                    "reconciled_canceled": stats["reconciled_canceled"],
                    "total_processed": stats["processed"],
                },
            )
        except Exception as e:
            logger.warning(
                "Failed to send drift alert",
                error=str(e),
            )
    else:
        logger.info(
            "Payment reconciliation completed - no drift found",
            **stats,
        )

    stats["org_results"] = org_results
    return stats


async def _reconcile_org_payments(session_maker, org: Organization) -> dict[str, Any]:
    """
    Reconcile payments for a single organization.

    Args:
        session_maker: Database session factory
        org: Organization to reconcile

    Returns:
        Dict with org-specific reconciliation statistics
    """
    org_stats = {
        "processed": 0,
        "reconciled_succeeded": 0,
        "reconciled_failed": 0,
        "reconciled_canceled": 0,
        "already_correct": 0,
        "errors": 0,
    }

    # Fetch org with Stripe key in its own session (may trigger decryption)
    async with session_maker() as key_db:
        fresh_org = await key_db.get(Organization, org.id)
        stripe_key = fresh_org.stripe_secret_key if fresh_org else None

    if not stripe_key:
        logger.warning(
            "Organization has no Stripe key configured, skipping",
            org_id=org.id,
            org_slug=org.slug,
        )
        return org_stats

    async with session_maker() as db:
        try:
            # Find PENDING payments older than threshold
            threshold_time = datetime.now(UTC) - timedelta(
                minutes=RECONCILIATION_THRESHOLD_MINUTES
            )

            # Find payments in any incomplete Stripe status
            incomplete_statuses = [
                PaymentStatus.requires_payment_method,
                PaymentStatus.requires_confirmation,
                PaymentStatus.requires_action,
                PaymentStatus.processing,
            ]
            result = await db.execute(
                select(Payment).where(
                    and_(
                        Payment.org_id == org.id,
                        Payment.status.in_(incomplete_statuses),
                        Payment.created_at < threshold_time,
                        Payment.external_payment_id.isnot(None),
                    )
                )
            )
            stale_payments = list(result.scalars().all())

            if not stale_payments:
                logger.debug(
                    "No stale payments to reconcile",
                    org_id=org.id,
                )
                return org_stats

            logger.info(
                "Found stale payments to reconcile",
                org_id=org.id,
                count=len(stale_payments),
            )

            for payment in stale_payments:
                org_stats["processed"] += 1

                try:
                    result = await _reconcile_single_payment(
                        db, payment, stripe_key, org.id
                    )

                    if result == "succeeded":
                        org_stats["reconciled_succeeded"] += 1
                    elif result == "failed":
                        org_stats["reconciled_failed"] += 1
                    elif result == "canceled":
                        org_stats["reconciled_canceled"] += 1
                    elif result == "correct":
                        org_stats["already_correct"] += 1
                    else:
                        org_stats["errors"] += 1

                    await db.commit()

                except Exception as e:
                    await db.rollback()
                    org_stats["errors"] += 1
                    logger.error(
                        "Error reconciling payment",
                        payment_id=payment.id,
                        org_id=org.id,
                        error=str(e),
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(
                "Error processing org payments for reconciliation",
                org_id=org.id,
                error=str(e),
                exc_info=True,
            )
            org_stats["errors"] += 1

    return org_stats


async def _reconcile_single_payment(
    db, payment: Payment, stripe_key: str, org_id: int
) -> str:
    """
    Reconcile a single payment against Stripe.

    Args:
        db: Database session
        payment: Payment to reconcile
        stripe_key: Stripe secret key for API calls
        org_id: Organization ID

    Returns:
        Result status: "succeeded", "failed", "canceled", "correct", or "error"
    """
    payment_intent_id = payment.external_payment_id
    if not payment_intent_id:
        logger.warning(
            "Payment has no external_payment_id, skipping",
            payment_id=payment.id,
        )
        return "error"

    logger.info(
        "Checking Stripe status for stale payment",
        payment_id=payment.id,
        payment_intent_id=payment_intent_id,
        org_id=org_id,
        created_at=payment.created_at.isoformat() if payment.created_at else None,
    )

    try:
        # Retrieve PaymentIntent from Stripe. Expand latest_charge so we
        # get payment_method_details.type for method detection and the
        # charge ID for transaction_id — all in one request.
        intent = stripe.PaymentIntent.retrieve(
            payment_intent_id,
            api_key=stripe_key,
            expand=["latest_charge"],
        )

        stripe_status = intent.status

        logger.info(
            "Stripe PaymentIntent status retrieved",
            payment_id=payment.id,
            payment_intent_id=payment_intent_id,
            stripe_status=stripe_status,
            db_status=payment.status.value,
        )

        if stripe_status == "succeeded":
            # Payment succeeded on Stripe but DB shows PENDING - CRITICAL DRIFT
            logger.warning(
                "DRIFT DETECTED: Payment succeeded on Stripe but PENDING in DB",
                payment_id=payment.id,
                payment_intent_id=payment_intent_id,
                org_id=org_id,
            )

            # Use PaymentOrchestrator to confirm atomically.
            # latest_charge is expanded (Charge object) from the retrieve call.
            orchestrator = PaymentOrchestrator(db, org_id)
            charge = intent.latest_charge
            transaction_id: str | None = None
            detected_method: PaymentMethod | None = None
            if charge and not isinstance(charge, str):
                transaction_id = charge.id
                pmd = getattr(charge, "payment_method_details", None)
                if pmd:
                    payment_method_map: dict[str, PaymentMethod] = {
                        "card": PaymentMethod.CREDIT_CARD,
                        "us_bank_account": PaymentMethod.BANK_TRANSFER,
                        "link": PaymentMethod.CREDIT_CARD,
                    }
                    detected_method = payment_method_map.get(
                        getattr(pmd, "type", ""), None
                    )

            result = await orchestrator.confirm_payment_atomic(
                payment_intent_id=payment_intent_id,
                triggered_by="reconciliation",
                transaction_id=transaction_id,
                payment_method=detected_method,
            )

            if result.already_processed:
                logger.info(
                    "Payment already confirmed (race condition resolved)",
                    payment_id=payment.id,
                )
                return "correct"
            elif result.error:
                logger.error(
                    "Failed to reconcile payment",
                    payment_id=payment.id,
                    error=result.error,
                )
                return "error"
            else:
                logger.info(
                    "Payment reconciled successfully",
                    payment_id=payment.id,
                    registrations_confirmed=result.registrations_confirmed,
                )

                # ACH (BANK_TRANSFER) receipts: mark available now that
                # Stripe has confirmed success. The webhook path handles
                # this in handle_webhook_event, but reconciliation bypasses
                # that code path.
                reconciled_payment = result.payment
                if (
                    reconciled_payment
                    and reconciled_payment.method == PaymentMethod.BANK_TRANSFER
                    and not reconciled_payment.receipt_available
                ):
                    reconciled_payment.receipt_available = True
                    await db.flush()

                # Safety net: enqueue confirmation email via outbox.
                # If endpoint + webhook already enqueued, the dedup key
                # makes this a no-op.
                if reconciled_payment and (
                    reconciled_payment.batch_id or reconciled_payment.registration_id
                ):
                    from app.models.user import User
                    from app.services.email_orchestrator import EmailOrchestrator

                    user = await db.get(User, reconciled_payment.user_id)
                    if not user:
                        logger.error(
                            "User not found for reconciled payment; skipping confirmation email enqueue",
                            payment_id=reconciled_payment.id,
                            user_id=reconciled_payment.user_id,
                            org_id=org_id,
                        )
                    else:
                        email_orch = EmailOrchestrator(db)
                        await email_orch.enqueue_program_confirmation(
                            batch_id=reconciled_payment.batch_id,
                            registration_id=reconciled_payment.registration_id,
                            user_id=reconciled_payment.user_id,
                            org_id=org_id,
                            to_email=user.email,
                        )

                return "succeeded"

        elif stripe_status == "canceled":
            # Payment was canceled on Stripe
            logger.info(
                "Marking canceled payment in DB",
                payment_id=payment.id,
                payment_intent_id=payment_intent_id,
            )
            payment.status = PaymentStatus.canceled
            payment.confirmed_by = "reconciliation"
            payment.confirmed_at = datetime.now(UTC)
            await db.flush()
            return "canceled"

        elif stripe_status in ("requires_payment_method", "requires_confirmation"):
            # Payment not yet attempted - still waiting
            # This is normal for abandoned checkouts, no action needed yet
            logger.debug(
                "Payment still awaiting completion on Stripe",
                payment_id=payment.id,
                stripe_status=stripe_status,
            )
            return "correct"

        elif stripe_status == "processing":
            # Payment is processing - leave it alone
            logger.debug(
                "Payment still processing on Stripe",
                payment_id=payment.id,
            )
            return "correct"

        elif stripe_status in ("requires_action", "requires_capture"):
            # Payment needs user action or capture - log but don't change
            logger.info(
                "Payment requires action on Stripe",
                payment_id=payment.id,
                stripe_status=stripe_status,
            )
            return "correct"

        else:
            # Unknown status - log for investigation
            logger.warning(
                "Unknown Stripe status during reconciliation",
                payment_id=payment.id,
                stripe_status=stripe_status,
            )
            return "error"

    except stripe.InvalidRequestError as e:
        # PaymentIntent not found or invalid
        if "No such payment_intent" in str(e):
            logger.warning(
                "PaymentIntent not found on Stripe - marking as failed",
                payment_id=payment.id,
                payment_intent_id=payment_intent_id,
            )
            payment.status = PaymentStatus.failed
            payment.failure_reason = "PaymentIntent not found on Stripe"
            payment.confirmed_by = "reconciliation"
            payment.confirmed_at = datetime.now(UTC)
            await db.flush()
            return "failed"
        else:
            logger.error(
                "Stripe error during reconciliation",
                payment_id=payment.id,
                error=str(e),
            )
            return "error"

    except stripe.AuthenticationError as e:
        logger.error(
            "Stripe authentication error - check API key",
            org_id=org_id,
            error=str(e),
        )
        return "error"

    except Exception as e:
        logger.error(
            "Unexpected error during payment reconciliation",
            payment_id=payment.id,
            error=str(e),
            exc_info=True,
        )
        return "error"


async def cleanup_abandoned_payments(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Clean up very old PENDING payments that were never completed.

    This task runs less frequently (e.g., daily) to mark payments
    that have been PENDING for more than 24 hours as CANCELED.

    Args:
        ctx: ARQ context with session_maker

    Returns:
        Dict with cleanup statistics
    """
    logger.info("Starting abandoned payment cleanup task...")

    session_maker = ctx.get("session_maker")

    if not session_maker:
        logger.error("Missing session_maker in context")
        return {"error": "Missing dependencies", "cleaned": 0}

    stats = {
        "cleaned": 0,
        "errors": 0,
        "timestamp": utc_now_iso(),
    }

    # Payments older than 24 hours are considered abandoned
    threshold_time = datetime.now(UTC) - timedelta(hours=24)

    async with session_maker() as db:
        try:
            # Find very old incomplete payments (abandoned intents)
            incomplete_statuses = [
                PaymentStatus.requires_payment_method,
                PaymentStatus.requires_confirmation,
                PaymentStatus.requires_action,
            ]
            result = await db.execute(
                select(Payment).where(
                    and_(
                        Payment.status.in_(incomplete_statuses),
                        Payment.created_at < threshold_time,
                    )
                )
            )
            abandoned_payments = list(result.scalars().all())

            if not abandoned_payments:
                logger.info("No abandoned payments to clean up")
                return stats

            logger.info(
                "Found abandoned payments to clean up",
                count=len(abandoned_payments),
            )

            for payment in abandoned_payments:
                try:
                    # Cancel on Stripe if possible
                    stripe_key = None
                    if payment.org_id:
                        org_result = await db.execute(
                            select(Organization).where(
                                Organization.id == payment.org_id
                            )
                        )
                        org = org_result.scalar_one_or_none()
                        if org:
                            stripe_key = org.stripe_secret_key

                    # If there's a live PaymentIntent but no key to cancel it,
                    # skip this payment to avoid DB/Stripe divergence.
                    if payment.external_payment_id and not stripe_key:
                        logger.warning(
                            "Skipping abandoned payment cleanup - no Stripe key to cancel intent",
                            payment_id=payment.id,
                            payment_intent_id=payment.external_payment_id,
                            org_id=payment.org_id,
                        )
                        stats["errors"] += 1
                        continue

                    if stripe_key and payment.external_payment_id:
                        try:
                            stripe.PaymentIntent.cancel(
                                payment.external_payment_id,
                                api_key=stripe_key,
                            )
                            logger.info(
                                "Canceled abandoned PaymentIntent on Stripe",
                                payment_id=payment.id,
                                payment_intent_id=payment.external_payment_id,
                            )
                        except stripe.InvalidRequestError:
                            # Already canceled or can't be canceled
                            pass
                        except Exception as e:
                            logger.warning(
                                "Failed to cancel PaymentIntent on Stripe",
                                payment_id=payment.id,
                                error=str(e),
                            )
                            # Don't mark DB as canceled if Stripe cancel failed
                            stats["errors"] += 1
                            continue

                    # Mark as canceled in DB — safe because either:
                    # (a) Stripe cancel succeeded, or
                    # (b) no external_payment_id (nothing to diverge from)
                    payment.status = PaymentStatus.canceled
                    payment.failure_reason = (
                        "Abandoned - payment not completed within 24 hours"
                    )
                    payment.confirmed_by = "cleanup"
                    payment.confirmed_at = datetime.now(UTC)
                    stats["cleaned"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(
                        "Error cleaning up abandoned payment",
                        payment_id=payment.id,
                        error=str(e),
                    )

            await db.commit()

            logger.info(
                "Abandoned payment cleanup completed",
                **stats,
            )

        except Exception as e:
            logger.error(
                "Error during abandoned payment cleanup",
                error=str(e),
                exc_info=True,
            )
            stats["errors"] += 1

    return stats
