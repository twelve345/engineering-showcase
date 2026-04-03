"""Tests for shadow mode: TOTP setup, shadow sessions, handoff, and middleware."""

import uuid

import pyotp
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.encryption import encrypt_pii
from app.models.tenant import Tenant
from app.models.user import User
from app.models.user_tenant import UserTenant, UserTenantRole
from tests.conftest import architect_auth_headers


@pytest.fixture
async def target_user(db_session: AsyncSession, test_tenant: Tenant) -> User:
    """Create a target user with a tenant membership."""
    user = User(
        id=uuid.uuid4(),
        email=f"target-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password="$2b$12$test_hashed_password",
        first_name="Target",
        last_name="User",
        is_verified=True,
        is_active=True,
    )
    db_session.add(user)
    await db_session.flush()

    membership = UserTenant(
        user_id=user.id,
        tenant_id=test_tenant.id,
        role=UserTenantRole.CLIENT,
    )
    db_session.add(membership)
    await db_session.flush()
    return user


# ── TOTP Setup ──


@pytest.mark.asyncio
async def test_totp_status_not_configured(
    client: AsyncClient,
    architect_user: User,
) -> None:
    headers = architect_auth_headers(architect_user.id)
    resp = await client.get("/api/v1/architect/totp/status", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["is_configured"] is False


@pytest.mark.asyncio
async def test_totp_setup_returns_secret_and_uri(
    client: AsyncClient,
    architect_user: User,
) -> None:
    headers = architect_auth_headers(architect_user.id)
    resp = await client.post("/api/v1/architect/totp/setup", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "secret" in data
    assert "provisioning_uri" in data
    assert "SessionReserve" in data["provisioning_uri"]


@pytest.mark.asyncio
async def test_totp_confirm_saves_secret(
    client: AsyncClient,
    architect_user: User,
    mock_encryption_keys: None,
) -> None:
    headers = architect_auth_headers(architect_user.id)

    # Setup
    setup_resp = await client.post("/api/v1/architect/totp/setup", headers=headers)
    secret = setup_resp.json()["secret"]

    # Generate valid code
    code = pyotp.TOTP(secret).now()

    # Confirm
    resp = await client.post(
        "/api/v1/architect/totp/confirm",
        headers=headers,
        json={"secret": secret, "totp_code": code},
    )
    assert resp.status_code == 200
    assert "confirmed_at" in resp.json()

    # Status should now show configured
    status_resp = await client.get("/api/v1/architect/totp/status", headers=headers)
    assert status_resp.json()["is_configured"] is True


@pytest.mark.asyncio
async def test_totp_confirm_invalid_code_rejected(
    client: AsyncClient,
    architect_user: User,
) -> None:
    headers = architect_auth_headers(architect_user.id)
    setup_resp = await client.post("/api/v1/architect/totp/setup", headers=headers)
    secret = setup_resp.json()["secret"]

    resp = await client.post(
        "/api/v1/architect/totp/confirm",
        headers=headers,
        json={"secret": secret, "totp_code": "000000"},
    )
    assert resp.status_code == 400


# ── Shadow Session Start ──


@pytest.fixture
async def architect_with_totp(
    architect_user: User,
    db_session: AsyncSession,
    mock_encryption_keys: None,
) -> tuple[User, str]:
    """Architect with TOTP configured. Returns (user, plaintext_secret)."""
    secret = pyotp.random_base32()
    architect_user.totp_secret = encrypt_pii(secret)
    await db_session.flush()
    return architect_user, secret


@pytest.mark.asyncio
async def test_shadow_start_success(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    target_user: User,
    test_tenant: Tenant,
) -> None:
    architect, secret = architect_with_totp
    headers = architect_auth_headers(architect.id)
    code = pyotp.TOTP(secret).now()

    resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": code,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "handoff_url" in data
    assert data["session"]["target_user_email"] == target_user.email
    assert data["session"]["target_tenant_slug"] == test_tenant.slug


@pytest.mark.asyncio
async def test_shadow_start_without_totp_rejected(
    client: AsyncClient,
    architect_user: User,
    target_user: User,
    test_tenant: Tenant,
) -> None:
    headers = architect_auth_headers(architect_user.id)
    resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": "123456",
        },
    )
    assert resp.status_code == 403
    assert "TOTP not configured" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_shadow_start_invalid_totp_rejected(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    target_user: User,
    test_tenant: Tenant,
) -> None:
    architect, _secret = architect_with_totp
    headers = architect_auth_headers(architect.id)

    resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": "000000",
        },
    )
    assert resp.status_code == 403
    assert "Invalid TOTP" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_shadow_start_self_shadow_rejected(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    test_tenant: Tenant,
    db_session: AsyncSession,
) -> None:
    architect, secret = architect_with_totp
    headers = architect_auth_headers(architect.id)

    # Give architect a membership so the validation gets past "not a member"
    membership = UserTenant(
        user_id=architect.id,
        tenant_id=test_tenant.id,
        role=UserTenantRole.STUDIO_ADMIN,
    )
    db_session.add(membership)
    await db_session.flush()

    code = pyotp.TOTP(secret).now()
    resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(architect.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": code,
        },
    )
    assert resp.status_code == 400
    assert "Cannot shadow yourself" in resp.json()["detail"]


# ── Handoff Exchange ──


@pytest.mark.asyncio
async def test_handoff_exchange_success(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    target_user: User,
    test_tenant: Tenant,
) -> None:
    architect, secret = architect_with_totp
    headers = architect_auth_headers(architect.id)
    code = pyotp.TOTP(secret).now()

    # Start session
    start_resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": code,
        },
    )
    handoff_url = start_resp.json()["handoff_url"]

    # Extract handoff token from URL
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(handoff_url)
    params = parse_qs(parsed.query)
    handoff_token = params["shadow_handoff"][0]

    # Exchange
    exchange_resp = await client.post(
        "/api/v1/auth/exchange-shadow-handoff",
        json={"handoff_token": handoff_token},
    )
    assert exchange_resp.status_code == 200
    data = exchange_resp.json()
    assert "shadow_token" in data
    assert data["session"]["target_user_email"] == target_user.email


@pytest.mark.asyncio
async def test_handoff_exchange_already_used(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    target_user: User,
    test_tenant: Tenant,
) -> None:
    architect, secret = architect_with_totp
    headers = architect_auth_headers(architect.id)
    code = pyotp.TOTP(secret).now()

    start_resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": code,
        },
    )

    from urllib.parse import parse_qs, urlparse

    handoff_token = parse_qs(urlparse(start_resp.json()["handoff_url"]).query)[
        "shadow_handoff"
    ][0]

    # First exchange succeeds
    resp1 = await client.post(
        "/api/v1/auth/exchange-shadow-handoff",
        json={"handoff_token": handoff_token},
    )
    assert resp1.status_code == 200

    # Second exchange fails
    resp2 = await client.post(
        "/api/v1/auth/exchange-shadow-handoff",
        json={"handoff_token": handoff_token},
    )
    assert resp2.status_code == 401
    assert "already been used" in resp2.json()["detail"]


# ── Shadow End ──


@pytest.mark.asyncio
async def test_shadow_end_success(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    target_user: User,
    test_tenant: Tenant,
) -> None:
    architect, secret = architect_with_totp
    headers = architect_auth_headers(architect.id)
    code = pyotp.TOTP(secret).now()

    start_resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": code,
        },
    )
    session_id = start_resp.json()["session"]["session_id"]

    end_resp = await client.post(
        "/api/v1/architect/shadow/end",
        headers=headers,
        json={"session_id": session_id},
    )
    assert end_resp.status_code == 200
    assert end_resp.json()["session_id"] == session_id
    assert "ended_at" in end_resp.json()


# ── Middleware Mutation Blocking ──


@pytest.mark.asyncio
async def test_shadow_middleware_blocks_mutations(
    client: AsyncClient,
    architect_with_totp: tuple[User, str],
    target_user: User,
    test_tenant: Tenant,
) -> None:
    architect, secret = architect_with_totp
    headers = architect_auth_headers(architect.id)
    code = pyotp.TOTP(secret).now()

    # Start session and exchange handoff
    start_resp = await client.post(
        "/api/v1/architect/shadow/start",
        headers=headers,
        json={
            "target_user_id": str(target_user.id),
            "target_tenant_id": str(test_tenant.id),
            "totp_code": code,
        },
    )

    from urllib.parse import parse_qs, urlparse

    handoff_token = parse_qs(urlparse(start_resp.json()["handoff_url"]).query)[
        "shadow_handoff"
    ][0]

    exchange_resp = await client.post(
        "/api/v1/auth/exchange-shadow-handoff",
        json={"handoff_token": handoff_token},
    )
    shadow_token = exchange_resp.json()["shadow_token"]
    shadow_headers = {"Authorization": f"Bearer {shadow_token}"}

    # GET should pass through
    get_resp = await client.get("/api/v1/health", headers=shadow_headers)
    assert get_resp.status_code == 200

    # POST to a random endpoint should be blocked
    post_resp = await client.post(
        "/api/v1/architect/tenants",
        headers=shadow_headers,
        json={"name": "Test", "slug": "test"},
    )
    assert post_resp.status_code == 403
    assert "blocked in shadow mode" in post_resp.json()["detail"]

    # POST to end-shadow-session should be whitelisted by middleware
    # AND succeed (accepts shadow JWT directly, no current_architect needed)
    end_resp = await client.post(
        "/api/v1/auth/end-shadow-session",
        headers=shadow_headers,
    )
    assert end_resp.status_code == 200
    assert "ended_at" in end_resp.json()


@pytest.mark.asyncio
async def test_shadow_middleware_allows_normal_requests(
    client: AsyncClient,
    architect_user: User,
) -> None:
    """Non-shadow tokens should pass through middleware without interference."""
    headers = architect_auth_headers(architect_user.id)
    resp = await client.get("/api/v1/architect/totp/status", headers=headers)
    assert resp.status_code == 200
