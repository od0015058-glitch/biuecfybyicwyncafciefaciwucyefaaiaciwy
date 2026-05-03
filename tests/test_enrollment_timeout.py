"""Tests for ``enrollment_timeout`` — DB-backed override for
ADMIN_2FA_ENROLLMENT_TIMEOUT.

Stage-15-Step-E #10b row 26.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import enrollment_timeout as et


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    """Clear module-level cache before each test."""
    monkeypatch.setattr(et, "_ENROLLMENT_TIMEOUT_OVERRIDE", None)
    monkeypatch.delenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", raising=False)
    yield


# ── coercion ─────────────────────────────────────────────────────


class TestCoercion:
    @pytest.mark.parametrize("value,expected", [
        (30, 30),
        (300, 300),
        (600, 600),
        (3600, 3600),
        ("30", 30),
        ("300", 300),
        ("3600", 3600),
        (60.0, 60),
        (300.0, 300),
    ])
    def test_happy(self, value, expected):
        assert et._coerce_enrollment_timeout(value) == expected

    @pytest.mark.parametrize("value", [
        True,
        False,
        "abc",
        "",
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        29,         # below min (30)
        3601,       # above max (3600)
        0,
        -1,
        60.5,       # non-integer float
        "60.5",     # non-integer float string
        "60abc",    # garbage suffix
        [60],       # list not supported
        {"value": 60},
    ])
    def test_rejection(self, value):
        assert et._coerce_enrollment_timeout(value) is None

    def test_whitespace_string_accepted(self):
        assert et._coerce_enrollment_timeout(" 60 ") == 60
        assert et._coerce_enrollment_timeout("\t300\n") == 300


# ── override accessors ───────────────────────────────────────────


class TestOverrideAccessors:
    def test_initial(self):
        assert et.get_enrollment_timeout_override() is None

    def test_set_and_get(self):
        et.set_enrollment_timeout_override(600)
        assert et.get_enrollment_timeout_override() == 600

    def test_set_rejects_bool(self):
        with pytest.raises(ValueError, match="not bool"):
            et.set_enrollment_timeout_override(True)
        with pytest.raises(ValueError, match="not bool"):
            et.set_enrollment_timeout_override(False)

    def test_set_rejects_below_min(self):
        with pytest.raises(ValueError):
            et.set_enrollment_timeout_override(29)

    def test_set_rejects_above_max(self):
        with pytest.raises(ValueError):
            et.set_enrollment_timeout_override(3601)

    def test_set_rejects_zero(self):
        with pytest.raises(ValueError):
            et.set_enrollment_timeout_override(0)

    def test_set_rejects_negative(self):
        with pytest.raises(ValueError):
            et.set_enrollment_timeout_override(-60)

    def test_set_accepts_min(self):
        et.set_enrollment_timeout_override(30)
        assert et.get_enrollment_timeout_override() == 30

    def test_set_accepts_max(self):
        et.set_enrollment_timeout_override(3600)
        assert et.get_enrollment_timeout_override() == 3600

    def test_clear_returns_true_when_active(self):
        et.set_enrollment_timeout_override(600)
        assert et.clear_enrollment_timeout_override() is True

    def test_clear_returns_false_when_none(self):
        assert et.clear_enrollment_timeout_override() is False

    def test_clear_resets(self):
        et.set_enrollment_timeout_override(600)
        et.clear_enrollment_timeout_override()
        assert et.get_enrollment_timeout_override() is None


# ── resolution ───────────────────────────────────────────────────


class TestResolution:
    def test_default(self):
        assert et.get_enrollment_timeout_seconds() == 300
        assert et.get_enrollment_timeout_source() == "default"

    def test_env_wins(self, monkeypatch):
        monkeypatch.setenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", "600")
        assert et.get_enrollment_timeout_seconds() == 600
        assert et.get_enrollment_timeout_source() == "env"

    def test_override_wins(self, monkeypatch):
        monkeypatch.setenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", "600")
        et.set_enrollment_timeout_override(120)
        assert et.get_enrollment_timeout_seconds() == 120
        assert et.get_enrollment_timeout_source() == "db"

    def test_invalid_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", "garbage")
        assert et.get_enrollment_timeout_seconds() == 300
        assert et.get_enrollment_timeout_source() == "default"

    def test_below_min_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", "10")
        assert et.get_enrollment_timeout_seconds() == 300
        assert et.get_enrollment_timeout_source() == "default"

    def test_above_max_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", "9999")
        assert et.get_enrollment_timeout_seconds() == 300
        assert et.get_enrollment_timeout_source() == "default"

    def test_override_after_clear_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("ADMIN_2FA_ENROLLMENT_TIMEOUT", "120")
        et.set_enrollment_timeout_override(60)
        assert et.get_enrollment_timeout_seconds() == 60
        et.clear_enrollment_timeout_override()
        assert et.get_enrollment_timeout_seconds() == 120
        assert et.get_enrollment_timeout_source() == "env"


# ── refresh from DB ──────────────────────────────────────────────


class TestRefreshFromDB:
    @pytest.mark.asyncio
    async def test_none_db(self):
        result = await et.refresh_enrollment_timeout_override_from_db(None)
        assert result is None

    @pytest.mark.asyncio
    async def test_none_db_keeps_existing_cache(self):
        et.set_enrollment_timeout_override(600)
        result = await et.refresh_enrollment_timeout_override_from_db(None)
        assert result == 600
        assert et.get_enrollment_timeout_override() == 600

    @pytest.mark.asyncio
    async def test_no_row_clears(self):
        et.set_enrollment_timeout_override(600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value=None)
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result is None
        assert et.get_enrollment_timeout_override() is None

    @pytest.mark.asyncio
    async def test_valid_row(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="600")
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result == 600
        assert et.get_enrollment_timeout_override() == 600

    @pytest.mark.asyncio
    async def test_valid_int_row(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value=120)
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result == 120

    @pytest.mark.asyncio
    async def test_malformed_clears(self):
        et.set_enrollment_timeout_override(600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="garbage")
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result is None
        assert et.get_enrollment_timeout_override() is None

    @pytest.mark.asyncio
    async def test_below_min_clears(self):
        et.set_enrollment_timeout_override(600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="10")
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_above_max_clears(self):
        et.set_enrollment_timeout_override(600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="9999")
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_db_error_keeps_cache(self):
        et.set_enrollment_timeout_override(600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        result = await et.refresh_enrollment_timeout_override_from_db(
            db_mock
        )
        assert result == 600
        assert et.get_enrollment_timeout_override() == 600


# ── format_timeout_human ─────────────────────────────────────────


class TestFormatTimeoutHuman:
    @pytest.mark.parametrize("seconds,expected", [
        (30, "30s"),
        (59, "59s"),
        (60, "1m"),
        (90, "1m 30s"),
        (300, "5m"),
        (3600, "1h"),
        (3660, "1h 1m"),
    ])
    def test_format(self, seconds, expected):
        assert et.format_timeout_human(seconds) == expected


# ── constants ────────────────────────────────────────────────────


class TestConstants:
    def test_setting_key(self):
        assert et.ENROLLMENT_TIMEOUT_SETTING_KEY == "ADMIN_2FA_ENROLLMENT_TIMEOUT"

    def test_default(self):
        assert et.DEFAULT_ENROLLMENT_TIMEOUT_SECONDS == 300

    def test_minimum(self):
        assert et.ENROLLMENT_TIMEOUT_MINIMUM == 30

    def test_maximum(self):
        assert et.ENROLLMENT_TIMEOUT_MAXIMUM == 3600

    def test_min_boundary(self):
        assert et._coerce_enrollment_timeout(30) == 30
        assert et._coerce_enrollment_timeout(29) is None

    def test_max_boundary(self):
        assert et._coerce_enrollment_timeout(3600) == 3600
        assert et._coerce_enrollment_timeout(3601) is None


# ── web_admin integration ────────────────────────────────────────


class TestWebAdminIntegration:
    def test_route_registered(self):
        """The POST route for /admin/enroll_2fa/timeout exists."""
        import web_admin
        assert hasattr(web_admin, "enroll_2fa_timeout_post")

    def test_audit_slug_exists(self):
        """The audit slug is registered in AUDIT_ACTION_LABELS."""
        import web_admin
        assert "enroll_2fa_timeout_update" in web_admin.AUDIT_ACTION_LABELS

    def test_build_enrollment_timeout_view(self):
        """_build_enrollment_timeout_view returns expected keys."""
        import web_admin
        view = web_admin._build_enrollment_timeout_view()
        expected_keys = {
            "effective", "effective_human",
            "db", "db_human",
            "env", "env_human",
            "default", "default_human",
            "source",
            "minimum", "maximum",
        }
        assert set(view.keys()) == expected_keys
        assert view["source"] == "default"
        assert view["effective"] == 300
        assert view["db"] is None


# ── template ─────────────────────────────────────────────────────


class TestTemplate:
    def test_template_exists(self):
        import os
        path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "templates", "admin", "enroll_2fa.html",
        )
        assert os.path.isfile(path)

    def test_template_has_timeout_editor(self):
        import os
        path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "templates", "admin", "enroll_2fa.html",
        )
        content = open(path, encoding="utf-8").read()
        assert "enrollment timeout" in content.lower()
        assert "/admin/enroll_2fa/timeout" in content
        assert "csrf_token" in content

    def test_template_has_countdown(self):
        import os
        path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "templates", "admin", "enroll_2fa.html",
        )
        content = open(path, encoding="utf-8").read()
        assert "countdown" in content
        assert "auto-reload" in content.lower() or "location.reload" in content


# ── bundled bug fix: get_flash → pop_flash ───────────────────────


class TestBugFixPopFlash:
    """Stage-15-Step-E #10b row 26 bundled bug fix: ``memory_config_get``
    used the undefined ``get_flash(request)`` instead of
    ``pop_flash(request, response)``.  Pre-fix, saving a memory-config
    override and landing back on /admin/memory-config would 500 with
    ``NameError: name 'get_flash' is not defined``.
    """

    def test_no_get_flash_reference(self):
        """web_admin.py must not reference get_flash anywhere."""
        import os
        path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "web_admin.py",
        )
        content = open(path, encoding="utf-8").read()
        assert "get_flash(" not in content, (
            "web_admin.py still references the undefined 'get_flash' "
            "function — use 'pop_flash(request, response)' instead"
        )

    def test_memory_config_uses_pop_flash(self):
        """memory_config_get must use pop_flash, not get_flash."""
        import inspect
        import web_admin
        src = inspect.getsource(web_admin.memory_config_get)
        assert "pop_flash" in src
        assert "get_flash" not in src
