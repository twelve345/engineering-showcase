"""Tests for PII blind-index query helpers and blind-index sync listener."""

import base64
import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.encryption import compute_email_blind_index, compute_phone_blind_index
from app.core.pii_queries import (
    find_user_by_email,
    find_user_by_email_or_secondary,
    find_user_by_sms_phone,
)
from app.models.user import User
from tests.factories.user import UserFactory

pytestmark = pytest.mark.asyncio(loop_scope="session")

# ---------------------------------------------------------------------------
# Test-only keys (random per run, NOT real secrets)
# ---------------------------------------------------------------------------

_TEST_AES_KEY = base64.b64encode(os.urandom(32)).decode()
_TEST_BLIND_KEY = os.urandom(32).hex()

_CFG = "app.core.encryption.settings"


def _settings(**overrides: Any) -> MagicMock:
    """Build a mock settings namespace with sensible defaults."""
    defaults: dict[str, Any] = {
        "ENCRYPTION_KEY_V1": _TEST_AES_KEY,
        "ENCRYPTION_ACTIVE_VERSION": 1,
        "BLIND_INDEX_KEY": _TEST_BLIND_KEY,
    }
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


@pytest.fixture(autouse=True)
def _mock_encryption_settings():
    """Ensure encryption keys are available for all tests in this module."""
    with patch(_CFG, _settings()):
        yield


# ===========================================================================
# Blind-index sync tests (replaces dual-write tests)
# ===========================================================================


class TestBlindIndexSync:
    """Verify that the before_flush listener populates bidx columns."""

    async def test_insert_populates_bidx(self, db_session: AsyncSession) -> None:
        """INSERT should populate all _bidx columns."""
        user = await UserFactory.create(
            db_session,
            email="insert@example.com",
            first_name="Alice",
            last_name="Smith",
            sms_phone_number="+15551234567",
        )

        assert user.email_bidx == compute_email_blind_index("insert@example.com")
        assert user.sms_phone_number_bidx == compute_phone_blind_index("+15551234567")

    async def test_insert_nullable_pii_bidx_is_none(
        self, db_session: AsyncSession
    ) -> None:
        """INSERT with NULL optional PII should leave bidx columns NULL."""
        user = await UserFactory.create(
            db_session,
            email="no-phone@example.com",
            sms_phone_number=None,
            secondary_email=None,
        )

        assert user.sms_phone_number_bidx is None
        assert user.secondary_email_bidx is None
        assert user.email_bidx is not None

    async def test_update_pii_field_syncs_bidx(self, db_session: AsyncSession) -> None:
        """UPDATE to a PII field should re-sync bidx columns."""
        user = await UserFactory.create(
            db_session,
            email="original@example.com",
        )
        original_bidx = user.email_bidx

        user.email = "updated@example.com"
        await db_session.flush()

        assert user.email_bidx == compute_email_blind_index("updated@example.com")
        assert user.email_bidx != original_bidx

    async def test_update_non_pii_field_does_not_resync(
        self, db_session: AsyncSession
    ) -> None:
        """UPDATE to a non-PII field should NOT re-sync bidx columns."""
        user = await UserFactory.create(
            db_session,
            email="stable@example.com",
        )
        original_bidx = user.email_bidx

        user.is_active = False
        await db_session.flush()

        assert user.email_bidx == original_bidx


# ===========================================================================
# Query helper tests (bidx-only, no plaintext fallback)
# ===========================================================================


class TestFindUserByEmail:
    """Test blind-index lookup for email."""

    async def test_finds_user_via_blind_index(self, db_session: AsyncSession) -> None:
        user = await UserFactory.create(
            db_session,
            email="bidx-hit@example.com",
        )
        assert user.email_bidx is not None

        found = await find_user_by_email(db_session, "bidx-hit@example.com")
        assert found is not None
        assert found.id == user.id

    async def test_returns_none_when_not_found(self, db_session: AsyncSession) -> None:
        found = await find_user_by_email(db_session, "nobody@example.com")
        assert found is None


class TestFindUserByEmailOrSecondary:
    """Test blind-index lookup for primary + secondary email uniqueness checks."""

    async def test_finds_by_primary_bidx(self, db_session: AsyncSession) -> None:
        user = await UserFactory.create(
            db_session,
            email="primary@example.com",
        )

        found = await find_user_by_email_or_secondary(db_session, "primary@example.com")
        assert found is not None
        assert found.id == user.id

    async def test_finds_by_secondary_bidx(self, db_session: AsyncSession) -> None:
        user = await UserFactory.create(
            db_session,
            email="main@example.com",
            secondary_email="alt@example.com",
        )

        found = await find_user_by_email_or_secondary(db_session, "alt@example.com")
        assert found is not None
        assert found.id == user.id

    async def test_exclude_user_id(self, db_session: AsyncSession) -> None:
        """exclude_user_id should skip the specified user."""
        user = await UserFactory.create(
            db_session,
            email="self@example.com",
        )

        found = await find_user_by_email_or_secondary(
            db_session, "self@example.com", exclude_user_id=user.id
        )
        assert found is None


class TestFindUserBySmsPhone:
    """Test blind-index lookup for SMS phone."""

    async def test_finds_user_via_blind_index(self, db_session: AsyncSession) -> None:
        user = await UserFactory.create(
            db_session,
            email="phone-bidx@example.com",
            sms_phone_number="+15559876543",
        )
        assert user.sms_phone_number_bidx is not None

        found = await find_user_by_sms_phone(db_session, "+15559876543")
        assert found is not None
        assert found.id == user.id

    async def test_duplicate_phone_returns_first_by_id(
        self, db_session: AsyncSession
    ) -> None:
        """Multiple users with the same phone should return the lowest ID."""
        user1 = await UserFactory.create(
            db_session,
            email="dup-phone-1@example.com",
            sms_phone_number="+15553334444",
        )
        await UserFactory.create(
            db_session,
            email="dup-phone-2@example.com",
            sms_phone_number="+15553334444",
        )

        found = await find_user_by_sms_phone(db_session, "+15553334444")
        assert found is not None
        assert found.id == user1.id

    async def test_returns_none_when_not_found(self, db_session: AsyncSession) -> None:
        found = await find_user_by_sms_phone(db_session, "+10000000000")
        assert found is None
