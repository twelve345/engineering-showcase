"""Authentication service — login, token refresh, registration, verification."""

import uuid as _uuid

from pwdlib import PasswordHash
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.exceptions import (
    AuthenticationError,
    DuplicateEmailError,
    TenantNotFoundError,
)
from app.core.security import (
    TokenError,
    create_access_token,
    create_email_verification_token,
    create_handoff_token,
    create_refresh_token,
    verify_token,
)
from app.models.email_outbox import EmailOutbox
from app.models.tenant import Tenant
from app.models.user import User
from app.models.user_tenant import UserTenant, UserTenantRole
from app.schemas.auth import (
    HandoffResponse,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
)


async def _resolve_role(db: AsyncSession, user: User, tenant: Tenant) -> str:
    """Determine the user's role for a given tenant."""
    if user.is_architect:
        return "studio_admin"

    result = await db.execute(
        select(UserTenant).where(
            UserTenant.user_id == user.id,
            UserTenant.tenant_id == tenant.id,
        )
    )
    membership = result.scalar_one_or_none()
    if membership is None:
        raise AuthenticationError
    return membership.role.value


async def authenticate_architect(
    db: AsyncSession,
    email: str,
    password: str,
) -> TokenResponse:
    """Validate architect credentials and return tokens without tenant context."""
    email = email.strip().lower()

    user_result = await db.execute(select(User).where(User.email == email))
    user = user_result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise AuthenticationError

    if not PasswordHash.recommended().verify(password, user.hashed_password):
        raise AuthenticationError

    if not user.is_architect:
        raise AuthenticationError

    return TokenResponse(
        access_token=create_access_token(
            user_id=user.id, role="architect", is_architect=True
        ),
        refresh_token=create_refresh_token(user_id=user.id),
    )


async def authenticate(
    db: AsyncSession,
    email: str,
    password: str,
    tenant_slug: str,
) -> TokenResponse:
    """Validate credentials and return an access/refresh token pair."""
    email = email.strip().lower()

    # Look up user
    user_result = await db.execute(select(User).where(User.email == email))
    user = user_result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise AuthenticationError

    # Verify password
    if not PasswordHash.recommended().verify(password, user.hashed_password):
        raise AuthenticationError

    # Resolve tenant
    tenant_result = await db.execute(
        select(Tenant).where(Tenant.slug == tenant_slug, Tenant.is_active.is_(True))
    )
    tenant = tenant_result.scalar_one_or_none()
    if tenant is None:
        raise AuthenticationError

    role = await _resolve_role(db, user, tenant)

    return TokenResponse(
        access_token=create_access_token(
            user_id=user.id, tenant_id=tenant.id, role=role
        ),
        refresh_token=create_refresh_token(user_id=user.id, tenant_id=tenant.id),
    )


def _build_verification_url(tenant_slug: str, token: str) -> str:
    """Build the email verification URL based on environment."""
    if settings.ENVIRONMENT == "development":
        return f"http://{settings.FRONTEND_DOMAIN}/verify-email?token={token}&tenant={tenant_slug}"
    return (
        f"https://{tenant_slug}.{settings.FRONTEND_DOMAIN}/verify-email?token={token}"
    )


async def register_client(
    db: AsyncSession,
    data: RegisterRequest,
) -> RegisterResponse:
    """Register a new client account and queue a verification email."""
    email = data.email.strip().lower()

    # Check email uniqueness
    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none() is not None:
        raise DuplicateEmailError

    # Resolve tenant
    tenant_result = await db.execute(
        select(Tenant).where(
            Tenant.slug == data.tenant_slug,
            Tenant.is_active.is_(True),
        )
    )
    tenant = tenant_result.scalar_one_or_none()
    if tenant is None:
        raise AuthenticationError("Invalid studio")

    # Create user
    hashed_password = PasswordHash.recommended().hash(data.password)
    user = User(
        email=email,
        hashed_password=hashed_password,
        first_name=data.first_name,
        last_name=data.last_name,
        is_verified=False,
        is_active=True,
    )
    db.add(user)
    await db.flush()

    # Create tenant membership
    membership = UserTenant(
        user_id=user.id,
        tenant_id=tenant.id,
        role=UserTenantRole.CLIENT,
    )
    db.add(membership)
    await db.flush()

    # Queue verification email
    token = create_email_verification_token(user_id=user.id, tenant_id=tenant.id)
    verification_url = _build_verification_url(data.tenant_slug, token)

    outbox_row = EmailOutbox(
        tenant_id=tenant.id,
        to_email=email,
        subject="Verify your email",
        template="email_verification",
        context={
            "user_name": data.first_name,
            "verification_url": verification_url,
        },
    )
    db.add(outbox_row)
    await db.flush()

    return RegisterResponse(
        id=str(user.id),
        email=user.email,
        message="Check your email to verify your account",
    )


async def verify_email(db: AsyncSession, token_str: str) -> TokenResponse:
    """Verify a user's email address and return login tokens."""
    try:
        payload = verify_token(token_str)
    except TokenError:
        raise AuthenticationError("Invalid or expired verification link") from None

    if payload.get("type") != "email_verification":
        raise AuthenticationError("Invalid verification token")

    raw_user_id = payload.get("sub")
    raw_tenant_id = payload.get("tenant_id")
    if not raw_user_id or not raw_tenant_id:
        raise AuthenticationError("Invalid verification token")

    try:
        user_id = _uuid.UUID(str(raw_user_id))
        tenant_id = _uuid.UUID(str(raw_tenant_id))
    except ValueError:
        raise AuthenticationError("Invalid verification token") from None

    # Load user
    user_result = await db.execute(select(User).where(User.id == user_id))
    user = user_result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise AuthenticationError("Invalid verification token")

    # Mark as verified
    user.is_verified = True
    await db.flush()

    # Load the specific tenant membership from the token
    membership_result = await db.execute(
        select(UserTenant)
        .join(Tenant, UserTenant.tenant_id == Tenant.id)
        .where(
            UserTenant.user_id == user.id,
            UserTenant.tenant_id == tenant_id,
            Tenant.is_active.is_(True),
        )
    )
    membership = membership_result.scalar_one_or_none()
    if membership is None:
        raise AuthenticationError("No active studio membership found")

    return TokenResponse(
        access_token=create_access_token(
            user_id=user.id,
            tenant_id=membership.tenant_id,
            role=membership.role.value,
        ),
        refresh_token=create_refresh_token(
            user_id=user.id,
            tenant_id=membership.tenant_id,
        ),
    )


async def refresh_tokens(db: AsyncSession, refresh_token_str: str) -> TokenResponse:
    """Validate a refresh token and issue a new token pair."""
    try:
        payload = verify_token(refresh_token_str)
    except TokenError:
        raise AuthenticationError from None

    if payload.get("type") != "refresh":
        raise AuthenticationError

    raw_user_id = payload.get("sub")
    raw_tenant_id = payload.get("tenant_id")
    if not raw_user_id:
        raise AuthenticationError

    try:
        user_id = _uuid.UUID(str(raw_user_id))
    except ValueError:
        raise AuthenticationError from None

    # Validate user still exists and is active
    user_result = await db.execute(select(User).where(User.id == user_id))
    user = user_result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise AuthenticationError

    # Architect tokens have no tenant context
    if not raw_tenant_id:
        if not user.is_architect:
            raise AuthenticationError
        return TokenResponse(
            access_token=create_access_token(
                user_id=user.id, role="architect", is_architect=True
            ),
            refresh_token=create_refresh_token(user_id=user.id),
        )

    try:
        tenant_id = _uuid.UUID(str(raw_tenant_id))
    except ValueError:
        raise AuthenticationError from None

    # Validate tenant still exists and is active
    tenant_result = await db.execute(
        select(Tenant).where(Tenant.id == tenant_id, Tenant.is_active.is_(True))
    )
    tenant = tenant_result.scalar_one_or_none()
    if tenant is None:
        raise AuthenticationError

    role = await _resolve_role(db, user, tenant)

    return TokenResponse(
        access_token=create_access_token(
            user_id=user.id, tenant_id=tenant.id, role=role
        ),
        refresh_token=create_refresh_token(user_id=user.id, tenant_id=tenant.id),
    )


async def initiate_handoff(
    db: AsyncSession,
    architect: User,
    tenant_slug: str,
) -> HandoffResponse:
    """Create a handoff token for architect-to-tenant jump."""
    tenant_result = await db.execute(
        select(Tenant).where(Tenant.slug == tenant_slug, Tenant.is_active.is_(True))
    )
    tenant = tenant_result.scalar_one_or_none()
    if tenant is None:
        raise TenantNotFoundError(tenant_slug)

    token = create_handoff_token(
        architect_id=architect.id,
        tenant_slug=tenant.slug,
    )

    # Jump the architect into the tenant admin surface directly.
    if settings.ENVIRONMENT == "development":
        redirect_url = f"http://{settings.FRONTEND_DOMAIN}/admin?tenant={tenant.slug}&handoff={token}"
    else:
        redirect_url = (
            f"https://{tenant.slug}.{settings.FRONTEND_DOMAIN}/admin?handoff={token}"
        )

    return HandoffResponse(handoff_token=token, redirect_url=redirect_url)


async def exchange_handoff_token(
    db: AsyncSession,
    token_str: str,
) -> TokenResponse:
    """Exchange a handoff token for tenant-scoped session tokens."""
    try:
        payload = verify_token(token_str)
    except TokenError:
        raise AuthenticationError("Invalid or expired handoff token") from None

    if payload.get("type") != "handoff":
        raise AuthenticationError("Invalid handoff token")

    raw_user_id = payload.get("sub")
    tenant_slug = payload.get("tenant_slug")
    if not raw_user_id or not tenant_slug:
        raise AuthenticationError("Invalid handoff token")

    try:
        user_id = _uuid.UUID(str(raw_user_id))
    except ValueError:
        raise AuthenticationError("Invalid handoff token") from None

    # Validate architect
    user_result = await db.execute(select(User).where(User.id == user_id))
    user = user_result.scalar_one_or_none()
    if user is None or not user.is_active or not user.is_architect:
        raise AuthenticationError("Invalid handoff token")

    # Resolve tenant
    tenant_result = await db.execute(
        select(Tenant).where(
            Tenant.slug == str(tenant_slug), Tenant.is_active.is_(True)
        )
    )
    tenant = tenant_result.scalar_one_or_none()
    if tenant is None:
        raise TenantNotFoundError(str(tenant_slug))

    # Create tenant-scoped tokens for the architect
    role = await _resolve_role(db, user, tenant)

    return TokenResponse(
        access_token=create_access_token(
            user_id=user.id, tenant_id=tenant.id, role=role
        ),
        refresh_token=create_refresh_token(user_id=user.id, tenant_id=tenant.id),
    )
