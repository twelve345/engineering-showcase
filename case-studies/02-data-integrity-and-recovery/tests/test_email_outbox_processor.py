"""Tests for process_email_outbox two-phase commit processor."""

from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.email_outbox import EmailOutbox, EmailOutboxStatus
from app.models.organization import Organization
from app.tasks.email_outbox_tasks import process_email_outbox

pytestmark = pytest.mark.asyncio(loop_scope="session")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(session_maker):
    """Build the ARQ ctx dict expected by process_email_outbox."""
    return {"session_maker": session_maker}


def _session_maker_from(db: AsyncSession):
    """Wrap a test session in an async context manager matching session_maker()."""

    @asynccontextmanager
    async def _factory():
        yield db

    return _factory


async def _seed_outbox(
    db: AsyncSession,
    *,
    org_id: int,
    key: str = "test-key",
    method: str = "send_raw",
    payload: dict | str | None = None,
    status: EmailOutboxStatus = EmailOutboxStatus.PENDING,
    attempt_count: int = 0,
    max_attempts: int = 3,
    claimed_at: datetime | None = None,
) -> EmailOutbox:
    """Insert an outbox row directly via ORM for test setup."""
    if payload is None:
        payload = {
            "method": method,
            "to_email": "test@example.com",
            "subject": "Test",
            "html_body": "<p>Test</p>",
        }

    row = EmailOutbox(
        idempotency_key=key,
        email_type="test",
        to_email="test@example.com",
        subject="Test",
        org_id=org_id,
        payload=payload,
        status=status,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        claimed_at=claimed_at,
    )
    db.add(row)
    await db.flush()
    return row


# ---------------------------------------------------------------------------
# Tests — claim → send → SENT flow
# ---------------------------------------------------------------------------


async def test_successful_send_marks_sent(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """A PENDING row is claimed, dispatched successfully, and marked SENT."""
    row = await _seed_outbox(db_session, org_id=test_org.id, key="sent-ok")

    with patch(
        "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        mock_dispatch.return_value = True
        result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert result["sent"] == 1
    assert result["failed"] == 0
    assert result["abandoned"] == 0

    await db_session.refresh(row)
    assert row.status == EmailOutboxStatus.SENT
    assert row.sent_at is not None
    assert row.attempt_count == 1


async def test_no_pending_rows_returns_zeros(
    db_session: AsyncSession,
) -> None:
    """When there are no PENDING rows, returns all zeros without dispatching."""
    result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))
    assert result == {"sent": 0, "failed": 0, "abandoned": 0}


# ---------------------------------------------------------------------------
# Tests — send failure → retry (PENDING) and exhaustion (ABANDONED)
# ---------------------------------------------------------------------------


async def test_send_failure_resets_to_pending_for_retry(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """When dispatch returns False and attempts remain, row resets to PENDING."""
    row = await _seed_outbox(
        db_session, org_id=test_org.id, key="fail-retry", max_attempts=3
    )

    with patch(
        "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        mock_dispatch.return_value = False
        result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert result["failed"] == 1
    assert result["abandoned"] == 0

    await db_session.refresh(row)
    assert row.status == EmailOutboxStatus.PENDING
    assert row.attempt_count == 1
    assert row.claimed_at is None
    assert row.last_error == "EmailService returned False"


async def test_send_failure_abandons_after_max_attempts(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """When dispatch returns False and max_attempts reached, row is ABANDONED."""
    row = await _seed_outbox(
        db_session,
        org_id=test_org.id,
        key="fail-abandon",
        attempt_count=2,
        max_attempts=3,
    )

    with (
        patch(
            "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
            new_callable=AsyncMock,
        ) as mock_dispatch,
        patch("app.tasks.email_outbox_tasks._alert_abandoned", new_callable=AsyncMock),
    ):
        mock_dispatch.return_value = False
        result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert result["abandoned"] == 1
    assert result["failed"] == 0

    await db_session.refresh(row)
    assert row.status == EmailOutboxStatus.ABANDONED
    assert row.attempt_count == 3


# ---------------------------------------------------------------------------
# Tests — exception during dispatch → retry and exhaustion
# ---------------------------------------------------------------------------


async def test_dispatch_exception_resets_to_pending(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """When dispatch raises an exception and attempts remain, row resets to PENDING."""
    row = await _seed_outbox(
        db_session, org_id=test_org.id, key="exc-retry", max_attempts=3
    )

    with patch(
        "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        mock_dispatch.side_effect = RuntimeError("SMTP timeout")
        result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert result["failed"] == 1

    await db_session.refresh(row)
    assert row.status == EmailOutboxStatus.PENDING
    assert row.attempt_count == 1
    assert "RuntimeError" in (row.last_error or "")
    assert row.claimed_at is None


async def test_dispatch_exception_abandons_after_max_attempts(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """When dispatch raises and max_attempts reached, row is ABANDONED."""
    row = await _seed_outbox(
        db_session,
        org_id=test_org.id,
        key="exc-abandon",
        attempt_count=2,
        max_attempts=3,
    )

    with (
        patch(
            "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
            new_callable=AsyncMock,
        ) as mock_dispatch,
        patch("app.tasks.email_outbox_tasks._alert_abandoned", new_callable=AsyncMock),
    ):
        mock_dispatch.side_effect = ConnectionError("provider down")
        result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert result["abandoned"] == 1

    await db_session.refresh(row)
    assert row.status == EmailOutboxStatus.ABANDONED
    assert row.attempt_count == 3
    assert "ConnectionError" in (row.last_error or "")


# ---------------------------------------------------------------------------
# Tests — max_attempts boundary
# ---------------------------------------------------------------------------


async def test_row_at_max_attempts_is_not_claimed(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """A PENDING row whose attempt_count >= max_attempts is not claimed."""
    await _seed_outbox(
        db_session,
        org_id=test_org.id,
        key="at-max",
        attempt_count=3,
        max_attempts=3,
    )

    with patch(
        "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
        new_callable=AsyncMock,
    ) as mock_dispatch:
        result = await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert result == {"sent": 0, "failed": 0, "abandoned": 0}
    mock_dispatch.assert_not_awaited()


# ---------------------------------------------------------------------------
# Tests — Phase 1 marks PROCESSING
# ---------------------------------------------------------------------------


async def test_phase1_marks_rows_as_processing(
    db_session: AsyncSession, test_org: Organization
) -> None:
    """Phase 1 sets status to PROCESSING and sets claimed_at before dispatch."""
    await _seed_outbox(db_session, org_id=test_org.id, key="phase1-check")

    claimed_status: EmailOutboxStatus | None = None

    async def _capture_dispatch(db: AsyncSession, outbox: EmailOutbox) -> bool:
        nonlocal claimed_status
        # At this point the row should be PROCESSING
        await db.refresh(outbox)
        claimed_status = outbox.status
        return True

    with patch(
        "app.tasks.email_outbox_tasks.email_dispatch_service.dispatch",
        side_effect=_capture_dispatch,
    ):
        await process_email_outbox(_make_ctx(_session_maker_from(db_session)))

    assert claimed_status == EmailOutboxStatus.PROCESSING
