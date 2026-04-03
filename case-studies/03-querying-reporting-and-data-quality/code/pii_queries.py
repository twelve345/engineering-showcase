"""Blind-index query helpers for PII lookup.

These helpers use HMAC-SHA256 blind indexes for equality lookups on
encrypted PII columns.
"""

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.encryption import compute_email_blind_index, compute_phone_blind_index
from app.models.user import User


async def find_user_by_email(db: AsyncSession, email: str) -> User | None:
    """Look up a single user by primary email (blind index)."""
    normalized = email.strip().lower()
    bidx = compute_email_blind_index(normalized)
    stmt = select(User).where(User.email_bidx == bidx)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def find_user_by_email_or_secondary(
    db: AsyncSession, email: str, *, exclude_user_id: int | None = None
) -> User | None:
    """Check if *any* user holds ``email`` as primary or secondary.

    Used for uniqueness checks (e.g. adding a secondary email).
    """
    normalized = email.strip().lower()
    bidx = compute_email_blind_index(normalized)

    conditions = or_(User.email_bidx == bidx, User.secondary_email_bidx == bidx)
    if exclude_user_id is not None:
        stmt = select(User).where(User.id != exclude_user_id, conditions)
    else:
        stmt = select(User).where(conditions)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def find_user_by_sms_phone(db: AsyncSession, phone: str) -> User | None:
    """Look up a single user by SMS phone number (blind index).

    Phone numbers are not unique — multiple users may share the same number.
    Returns the first match in stable order (by user ID).
    """
    bidx = compute_phone_blind_index(phone)
    stmt = select(User).where(User.sms_phone_number_bidx == bidx).order_by(User.id)
    result = await db.execute(stmt)
    return result.scalars().first()
