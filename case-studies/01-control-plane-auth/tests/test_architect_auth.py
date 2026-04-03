"""Tests for architect auth, handoff, and refresh endpoints."""

import uuid
from datetime import timedelta

import pytest
from httpx import AsyncClient
from pwdlib import PasswordHash
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import (
    create_access_token,
    create_handoff_token,
    create_refresh_token,
)
from app.models.tenant import Tenant
from app.models.user import User
from tests.conftest import architect_auth_headers

TEST_PASSWORD = "architect-password-123"


@pytest.fixture
async def hashed_password() -> str:
    return PasswordHash.recommended().hash(TEST_PASSWORD)


@pytest.fixture
async def active_tenant(db_session: AsyncSession) -> Tenant:
    tenant = Tenant(
        id=uuid.uuid4(),
        slug="handoff-studio",
        name="Handoff Studio",
        timezone="America/New_York",
        is_active=True,
    )
    db_session.add(tenant)
    await db_session.flush()
    return tenant


@pytest.fixture
async def inactive_tenant(db_session: AsyncSession) -> Tenant:
    tenant = Tenant(
        id=uuid.uuid4(),
        slug="dead-studio",
        name="Dead Studio",
        timezone="America/New_York",
        is_active=False,
    )
    db_session.add(tenant)
    await db_session.flush()
    return tenant


@pytest.fixture
async def architect(db_session: AsyncSession, hashed_password: str) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"architect-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password=hashed_password,
        first_name="Platform",
        last_name="Architect",
        is_verified=True,
        is_active=True,
        is_architect=True,
    )
    db_session.add(user)
    await db_session.flush()
    return user


@pytest.fixture
async def non_architect(db_session: AsyncSession, hashed_password: str) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"regular-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password=hashed_password,
        first_name="Regular",
        last_name="User",
        is_verified=True,
        is_active=True,
        is_architect=False,
    )
    db_session.add(user)
    await db_session.flush()
    return user


@pytest.fixture
async def inactive_architect(db_session: AsyncSession, hashed_password: str) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"inactive-arch-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password=hashed_password,
        first_name="Inactive",
        last_name="Architect",
        is_verified=True,
        is_active=False,
        is_architect=True,
    )
    db_session.add(user)
    await db_session.flush()
    return user


# ── POST /auth/architect-login ──


@pytest.mark.asyncio
async def test_architect_login_success(client: AsyncClient, architect: User) -> None:
    resp = await client.post(
        "/api/v1/auth/architect-login",
        json={"email": architect.email, "password": TEST_PASSWORD},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert "refresh_token" in data


@pytest.mark.asyncio
async def test_architect_login_wrong_password(
    client: AsyncClient, architect: User
) -> None:
    resp = await client.post(
        "/api/v1/auth/architect-login",
        json={"email": architect.email, "password": "wrong-password"},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_architect_login_non_architect_user(
    client: AsyncClient, non_architect: User
) -> None:
    resp = await client.post(
        "/api/v1/auth/architect-login",
        json={"email": non_architect.email, "password": TEST_PASSWORD},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_architect_login_inactive_user(
    client: AsyncClient, inactive_architect: User
) -> None:
    resp = await client.post(
        "/api/v1/auth/architect-login",
        json={"email": inactive_architect.email, "password": TEST_PASSWORD},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_architect_login_unknown_email(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/auth/architect-login",
        json={"email": "nobody@example.com", "password": TEST_PASSWORD},
    )
    assert resp.status_code == 401


# ── POST /architect/handoff ──


@pytest.mark.asyncio
async def test_handoff_success(
    client: AsyncClient, architect: User, active_tenant: Tenant
) -> None:
    resp = await client.post(
        "/api/v1/architect/handoff",
        json={"tenant_slug": active_tenant.slug},
        headers=architect_auth_headers(architect.id),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "handoff_token" in data
    assert "redirect_url" in data
    assert data["redirect_url"].startswith("http://localhost:5173/admin?")
    assert f"tenant={active_tenant.slug}" in data["redirect_url"]
    assert "handoff=" in data["redirect_url"]


@pytest.mark.asyncio
async def test_handoff_requires_architect_auth(
    client: AsyncClient, active_tenant: Tenant
) -> None:
    resp = await client.post(
        "/api/v1/architect/handoff",
        json={"tenant_slug": active_tenant.slug},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_handoff_non_architect_token_rejected(
    client: AsyncClient, non_architect: User, active_tenant: Tenant
) -> None:
    """A regular user token must not grant access to the handoff endpoint."""
    token = create_access_token(
        user_id=non_architect.id,
        tenant_id=active_tenant.id,
        role="client",
    )
    headers = {"Authorization": f"Bearer {token}"}
    resp = await client.post(
        "/api/v1/architect/handoff",
        json={"tenant_slug": active_tenant.slug},
        headers=headers,
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_handoff_invalid_tenant_slug(
    client: AsyncClient, architect: User
) -> None:
    resp = await client.post(
        "/api/v1/architect/handoff",
        json={"tenant_slug": "nonexistent-studio"},
        headers=architect_auth_headers(architect.id),
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_handoff_inactive_tenant(
    client: AsyncClient, architect: User, inactive_tenant: Tenant
) -> None:
    resp = await client.post(
        "/api/v1/architect/handoff",
        json={"tenant_slug": inactive_tenant.slug},
        headers=architect_auth_headers(architect.id),
    )
    assert resp.status_code == 404


# ── POST /auth/handoff/exchange ──


@pytest.mark.asyncio
async def test_handoff_exchange_success(
    client: AsyncClient, architect: User, active_tenant: Tenant
) -> None:
    token = create_handoff_token(
        architect_id=architect.id,
        tenant_slug=active_tenant.slug,
    )
    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": token},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert "refresh_token" in data


@pytest.mark.asyncio
async def test_handoff_exchange_expired_token(
    client: AsyncClient, architect: User, active_tenant: Tenant
) -> None:
    """Handoff tokens have a 30-second TTL; an expired one must be rejected."""
    from datetime import UTC, datetime

    import jwt

    from app.core.config import settings

    now = datetime.now(UTC)
    payload = {
        "sub": str(architect.id),
        "tenant_slug": active_tenant.slug,
        "iat": now - timedelta(minutes=5),
        "exp": now - timedelta(minutes=4),
        "type": "handoff",
    }
    expired_token = jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")

    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": expired_token},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_handoff_exchange_wrong_token_type(
    client: AsyncClient, architect: User, active_tenant: Tenant
) -> None:
    """An access token must not be accepted as a handoff token."""
    access_token = create_access_token(
        user_id=architect.id, role="architect", is_architect=True
    )
    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": access_token},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_handoff_exchange_refresh_token_rejected(
    client: AsyncClient, architect: User
) -> None:
    """A refresh token must not be accepted as a handoff token."""
    refresh_token = create_refresh_token(user_id=architect.id)
    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": refresh_token},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_handoff_exchange_inactive_tenant(
    client: AsyncClient, architect: User, inactive_tenant: Tenant
) -> None:
    token = create_handoff_token(
        architect_id=architect.id,
        tenant_slug=inactive_tenant.slug,
    )
    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": token},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_handoff_exchange_garbage_token(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": "not.a.valid.token"},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_handoff_exchange_inactive_architect(
    client: AsyncClient, inactive_architect: User, active_tenant: Tenant
) -> None:
    """Handoff exchange must reject tokens for deactivated architects."""
    token = create_handoff_token(
        architect_id=inactive_architect.id,
        tenant_slug=active_tenant.slug,
    )
    resp = await client.post(
        "/api/v1/auth/handoff/exchange",
        json={"token": token},
    )
    assert resp.status_code == 401


# ── POST /auth/refresh with null tenant_id (architect refresh) ──


@pytest.mark.asyncio
async def test_architect_refresh_success(client: AsyncClient, architect: User) -> None:
    """Architect refresh tokens have no tenant_id; should return new tokens."""
    token = create_refresh_token(user_id=architect.id)
    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert "refresh_token" in data


@pytest.mark.asyncio
async def test_non_architect_refresh_with_null_tenant_id(
    client: AsyncClient, non_architect: User
) -> None:
    """A non-architect user with a null-tenant refresh token must be rejected."""
    token = create_refresh_token(user_id=non_architect.id)
    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_architect_refresh_after_deactivation(
    client: AsyncClient,
    db_session: AsyncSession,
    hashed_password: str,
) -> None:
    """Refresh must fail if the architect was deactivated after token issuance."""
    user = User(
        id=uuid.uuid4(),
        email=f"soon-deactivated-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password=hashed_password,
        first_name="Soon",
        last_name="Deactivated",
        is_verified=True,
        is_active=True,
        is_architect=True,
    )
    db_session.add(user)
    await db_session.flush()

    token = create_refresh_token(user_id=user.id)

    # Deactivate after token was issued
    user.is_active = False
    await db_session.flush()

    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token},
    )
    assert resp.status_code == 401
