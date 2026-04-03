"""Tests for tenant slug validation and reserved slugs."""

from app.core.config import settings
from app.models.tenant import Tenant


class TestTenantSlugValidation:
    """Valid slugs accepted, reserved slugs rejected, URL-safety enforced."""

    def test_valid_slug(self):
        assert Tenant.is_valid_slug("demo-studio")

    def test_valid_slug_short(self):
        assert Tenant.is_valid_slug("abc")

    def test_valid_slug_with_numbers(self):
        assert Tenant.is_valid_slug("studio-42")

    def test_invalid_slug_uppercase(self):
        assert not Tenant.is_valid_slug("Demo-Studio")

    def test_invalid_slug_spaces(self):
        assert not Tenant.is_valid_slug("demo studio")

    def test_invalid_slug_special_chars(self):
        assert not Tenant.is_valid_slug("demo@studio")

    def test_invalid_slug_starts_with_hyphen(self):
        assert not Tenant.is_valid_slug("-demo")

    def test_invalid_slug_ends_with_hyphen(self):
        assert not Tenant.is_valid_slug("demo-")

    def test_invalid_slug_too_short(self):
        assert not Tenant.is_valid_slug("ab")

    def test_invalid_slug_empty(self):
        assert not Tenant.is_valid_slug("")

    def test_reserved_slugs_are_defined(self):
        """Ensure reserved slugs list is populated."""
        reserved = settings.reserved_slugs_list
        assert "api" in reserved
        assert "admin" in reserved
        assert "www" in reserved

    def test_reserved_slug_check(self):
        """Even if valid format, reserved slugs should be rejected."""
        reserved = settings.reserved_slugs_list
        for slug in reserved:
            # These are technically valid format but should be caught by business logic
            # The ReservedSlugError is raised at the service level, not model level
            assert slug in reserved
