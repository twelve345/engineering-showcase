"""
Unit tests for payment reconciliation receipt_available handling.

Verifies that when the reconciliation task confirms a BANK_TRANSFER payment,
it sets receipt_available=True (matching the webhook path behavior).
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.organization import Organization
from app.models.payment import Payment, PaymentMethod, PaymentStatus
from app.models.user import User
from app.services.payment_orchestrator import PaymentConfirmationResult
from app.tasks.payment_reconciliation_tasks import _reconcile_single_payment


@dataclass
class FakePaymentIntent:
    status: str
    latest_charge: str | None = None


def _make_processing_payment(
    org_id: int,
    user_id: int,
    method: PaymentMethod,
) -> Payment:
    """Create a Payment object (not persisted) for reconciliation testing."""
    payment = Payment(
        org_id=org_id,
        user_id=user_id,
        amount=Decimal("120.00"),
        status=PaymentStatus.processing,
        method=method,
        currency="USD",
        external_payment_id=f"pi_{uuid4().hex[:24]}",
        receipt_available=False,
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    return payment


@pytest.mark.asyncio
async def test_reconcile_bank_transfer_sets_receipt_available(
    db_session: AsyncSession,
    test_org: Organization,
    test_user: User,
) -> None:
    """Reconciling a succeeded BANK_TRANSFER payment marks receipt_available=True."""
    payment = _make_processing_payment(
        test_org.id, test_user.id, PaymentMethod.BANK_TRANSFER
    )
    assert payment.receipt_available is False

    fake_intent = FakePaymentIntent(status="succeeded", latest_charge="ch_test_123")

    mock_result = PaymentConfirmationResult(
        already_processed=False,
        payment=payment,
        registrations_confirmed=0,
        scheduled_payments_created=0,
    )

    with (
        patch(
            "app.tasks.payment_reconciliation_tasks.stripe.PaymentIntent.retrieve",
            return_value=fake_intent,
        ),
        patch("app.tasks.payment_reconciliation_tasks.PaymentOrchestrator") as MockOrch,
    ):
        mock_instance = AsyncMock()
        mock_instance.confirm_payment_atomic = AsyncMock(return_value=mock_result)
        MockOrch.return_value = mock_instance

        result = await _reconcile_single_payment(
            db_session, payment, "sk_test_fake", test_org.id
        )

    assert result == "succeeded"
    assert payment.receipt_available is True


@pytest.mark.asyncio
async def test_reconcile_card_payment_does_not_change_receipt_available(
    db_session: AsyncSession,
    test_org: Organization,
    test_user: User,
) -> None:
    """Reconciling a CREDIT_CARD payment does not touch receipt_available."""
    payment = _make_processing_payment(
        test_org.id, test_user.id, PaymentMethod.CREDIT_CARD
    )

    fake_intent = FakePaymentIntent(status="succeeded", latest_charge="ch_test_456")

    mock_result = PaymentConfirmationResult(
        already_processed=False,
        payment=payment,
        registrations_confirmed=0,
        scheduled_payments_created=0,
    )

    with (
        patch(
            "app.tasks.payment_reconciliation_tasks.stripe.PaymentIntent.retrieve",
            return_value=fake_intent,
        ),
        patch("app.tasks.payment_reconciliation_tasks.PaymentOrchestrator") as MockOrch,
    ):
        mock_instance = AsyncMock()
        mock_instance.confirm_payment_atomic = AsyncMock(return_value=mock_result)
        MockOrch.return_value = mock_instance

        result = await _reconcile_single_payment(
            db_session, payment, "sk_test_fake", test_org.id
        )

    assert result == "succeeded"
    assert payment.receipt_available is False
