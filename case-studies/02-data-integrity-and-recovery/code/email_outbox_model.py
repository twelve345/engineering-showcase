"""Transactional email outbox for idempotent, centralized email sending.

Callers insert a row with a unique idempotency_key. An ARQ cron task
processes PENDING rows every 10 seconds. Duplicate inserts are no-ops
via ON CONFLICT (idempotency_key) DO NOTHING.
"""

import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, Index, Integer, String, event, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Mapped, Session, mapped_column

from app.core.encrypted_types import EncryptedJSON, EncryptedString
from app.core.encryption import compute_email_blind_index

from .base import BaseModel


class DispatchMethod(enum.Enum):
    PROGRAM_CONFIRMATION = "send_program_confirmation_email"
    PROMO_CODES = "send_promo_codes_email"
    PAYMENT_FAILURE = "send_payment_failure_email"
    TEACHER_WELCOME = "send_teacher_welcome_email"
    SCHEDULE_UPDATE = "send_schedule_update"
    WAITLIST_NOTIFICATION = "send_waitlist_notification"
    VERIFICATION_EMAIL = "send_verification_email"
    PASSWORD_RESET = "send_password_reset_email"
    SECONDARY_EMAIL_VERIFICATION = "send_secondary_email_verification"
    RAW = "send_raw"


class EmailOutboxStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"
    ABANDONED = "abandoned"


class EmailOutbox(BaseModel):
    __tablename__ = "email_outbox"

    # Core dedup mechanism
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)

    # Email metadata
    email_type: Mapped[str] = mapped_column(String(50), nullable=False)
    to_email: Mapped[str] = mapped_column(EncryptedString, nullable=False, default="")
    subject: Mapped[str] = mapped_column(EncryptedString, nullable=False, default="")

    # Multi-tenant scoping
    org_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Serialized EmailService method name + kwargs (IDs only)
    payload: Mapped[dict] = mapped_column(EncryptedJSON, nullable=False)

    # Status tracking
    status: Mapped[EmailOutboxStatus] = mapped_column(
        Enum(
            EmailOutboxStatus,
            name="emailoutboxstatus",
            create_type=False,
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=EmailOutboxStatus.PENDING,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    # Two-phase commit tracking
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Result tracking
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(EncryptedString)
    provider_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Blind index for recipient email lookups
    to_email_bidx: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_email_outbox_idempotency_key", "idempotency_key", unique=True),
        Index("ix_email_outbox_status_created", "status", "created_at"),
        Index("ix_email_outbox_org_id", "org_id"),
        Index(
            "ix_email_outbox_to_email_bidx",
            "to_email_bidx",
            postgresql_where=text("to_email_bidx IS NOT NULL"),
        ),
    )

    def _sync_blind_indexes(self) -> None:
        """Populate blind index columns from encrypted fields."""
        self.to_email_bidx = (
            compute_email_blind_index(self.to_email) if self.to_email else None
        )

    def __repr__(self) -> str:
        return f"<EmailOutbox {self.idempotency_key} {self.status.value}>"


_OUTBOX_PII_PLAINTEXT_ATTRS = frozenset({"to_email"})


@event.listens_for(Session, "before_flush")
def _outbox_bidx_before_flush(
    session: Session, flush_context: object, instances: object
) -> None:
    """Sync blind indexes before any EmailOutbox INSERT or UPDATE."""
    for obj in session.new:
        if isinstance(obj, EmailOutbox):
            obj._sync_blind_indexes()

    for obj in session.dirty:
        if not isinstance(obj, EmailOutbox):
            continue
        insp = sa_inspect(obj)
        for attr in _OUTBOX_PII_PLAINTEXT_ATTRS:
            hist = insp.attrs[attr].history
            if hist.has_changes():
                obj._sync_blind_indexes()
                break
