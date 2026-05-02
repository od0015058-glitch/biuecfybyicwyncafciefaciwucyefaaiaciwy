"""Tests for ``admin_password`` (Stage-15-Step-E #10b row 25).

Same shape as ``tests/test_model_discovery_config.py`` /
``tests/test_fx_refresh_config.py``: coercion, override accessors,
resolution order, refresh-from-DB. Plus a security-sensitive layer
testing the scrypt format, constant-time verify, and the wrong-
shape stored value behaviour.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest

import admin_password as ap


@pytest.fixture(autouse=True)
def _reset_admin_password_override(monkeypatch):
    """Each test starts with a clean cache + cleared env var so the
    fall-through chain is deterministic."""
    ap.clear_admin_password_hash_override()
    monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
    yield
    ap.clear_admin_password_hash_override()


# ------------------------------------------------------------------
# Strength validator
# ------------------------------------------------------------------

class TestPasswordStrength:
    """``validate_password_strength`` returns None on success, else
    a human-readable error string."""

    def test_valid_long_passphrase(self):
        # 24-char passphrase with letters + digit. Standard "long &
        # boring" pattern modern guidance recommends.
        assert ap.validate_password_strength(
            "CorrectHorseBatteryStaple7"
        ) is None

    def test_valid_short_with_symbol(self):
        # 12-char with letter + symbol — exactly at the floor.
        assert ap.validate_password_strength("abcdef-ghijk") is None

    def test_valid_with_digit(self):
        assert ap.validate_password_strength("abcdef1234567") is None

    @pytest.mark.parametrize("bad", [None, 12345, b"hello-world!", []])
    def test_non_string_rejected(self, bad):
        assert (
            ap.validate_password_strength(bad)
            == "password must be a string"
        )

    @pytest.mark.parametrize(
        "short",
        ["", "a", "abc", "12345", "ab1!", "abcdef12345"],  # all <12
    )
    def test_below_minimum_length(self, short):
        err = ap.validate_password_strength(short)
        assert err is not None
        assert "12" in err and "characters" in err

    def test_above_maximum_length(self):
        too_long = "a" * (ap.MAX_PASSWORD_LENGTH + 1) + "1"
        err = ap.validate_password_strength(too_long)
        assert err is not None
        assert str(ap.MAX_PASSWORD_LENGTH) in err

    def test_whitespace_only(self):
        # 12 spaces — at length floor but no real content.
        err = ap.validate_password_strength(" " * 12)
        assert err is not None
        assert "whitespace" in err.lower()

    def test_alpha_only_rejected(self):
        # 12 letters with no digit / symbol.
        err = ap.validate_password_strength("abcdefghijkl")
        assert err is not None
        assert "digit or symbol" in err.lower()

    def test_digit_only_rejected(self):
        # All digits — has "non-letter" but no letter.
        err = ap.validate_password_strength("1234567890123")
        assert err is not None
        assert "letter" in err.lower()

    def test_symbol_only_rejected(self):
        err = ap.validate_password_strength("!@#$%^&*()_+=")
        assert err is not None
        assert "letter" in err.lower()


# ------------------------------------------------------------------
# Hash format
# ------------------------------------------------------------------

class TestHashPassword:
    """``hash_password`` round-trips with ``verify_password`` and
    refuses to emit malformed parameters."""

    def test_round_trip(self):
        stored = ap.hash_password("CorrectHorseBatteryStaple7")
        assert ap.verify_password("CorrectHorseBatteryStaple7", stored)

    def test_wrong_plaintext_fails(self):
        stored = ap.hash_password("hunter2-actually")
        assert not ap.verify_password("Hunter2-actually", stored)
        assert not ap.verify_password("hunter2-actuall", stored)
        assert not ap.verify_password("", stored)

    def test_two_hashes_of_same_plaintext_differ(self):
        # Distinct salts → distinct stored values, even for the same
        # plaintext.
        a = ap.hash_password("samepassword-1")
        b = ap.hash_password("samepassword-1")
        assert a != b
        assert ap.verify_password("samepassword-1", a)
        assert ap.verify_password("samepassword-1", b)

    def test_hash_format(self):
        stored = ap.hash_password("longenoughpw-1")
        parts = stored.split("$")
        assert parts[0] == "scrypt"
        assert int(parts[1]) == ap.SCRYPT_N
        assert int(parts[2]) == ap.SCRYPT_R
        assert int(parts[3]) == ap.SCRYPT_P
        # Salt + digest are non-empty base64 strings.
        assert parts[4] and parts[5]

    def test_explicit_lower_cost_factors(self):
        # Tests use n=2 to keep the suite fast — mirrors how
        # production scrypt-backed test fixtures work.
        stored = ap.hash_password("testpw-12345", n=2, r=1, p=1)
        assert ap.verify_password("testpw-12345", stored)

    @pytest.mark.parametrize("bad", [None, 1234, b"bytes", []])
    def test_non_string_plaintext_raises(self, bad):
        with pytest.raises(TypeError):
            ap.hash_password(bad)

    @pytest.mark.parametrize("bad_n", [True, 1, 0, -1, 3, 5, 1.5])
    def test_non_power_of_two_n_raises(self, bad_n):
        with pytest.raises((ValueError, TypeError)):
            ap.hash_password("pw-1234567890ab", n=bad_n)

    @pytest.mark.parametrize("bad_r", [True, 0, -1, 1.5])
    def test_invalid_r_raises(self, bad_r):
        with pytest.raises((ValueError, TypeError)):
            ap.hash_password("pw-1234567890ab", r=bad_r)

    @pytest.mark.parametrize("bad_p", [True, 0, -1, 1.5])
    def test_invalid_p_raises(self, bad_p):
        with pytest.raises((ValueError, TypeError)):
            ap.hash_password("pw-1234567890ab", p=bad_p)


class TestVerifyPasswordParserSafety:
    """Malformed stored values must NEVER verify true. Returning
    ``False`` for "anything I don't understand" is the safe default —
    a parser bug must not silently accept passwords."""

    @pytest.mark.parametrize(
        "bad_stored",
        [
            "",
            "scrypt",
            "scrypt$",
            "scrypt$$$$",
            "bcrypt$2$8$1$abcd$efgh",
            "scrypt$abc$8$1$abcd$efgh",       # n not an int
            "scrypt$3$8$1$abcd$efgh",         # n not power of two
            "scrypt$2$0$1$abcd$efgh",         # r below 1
            "scrypt$2$8$0$abcd$efgh",         # p below 1
            "scrypt$2$8$1$NOT_BASE64!$NOT_BASE64!",
            "scrypt$2$8$1$$",                  # empty salt + hash
            f"scrypt${ap.SCRYPT_N_MAX * 2}$8$1$abcd$efgh",  # n too big
            f"scrypt$2${ap.SCRYPT_R_MAX + 1}$1$abcd$efgh",  # r too big
            f"scrypt$2$8${ap.SCRYPT_P_MAX + 1}$abcd$efgh",  # p too big
        ],
    )
    def test_malformed_stored_rejected(self, bad_stored):
        # "anything" plaintext including the empty string — none of
        # them should verify true.
        assert not ap.verify_password("anything", bad_stored)
        assert not ap.verify_password("", bad_stored)

    @pytest.mark.parametrize(
        "bad_plaintext", [None, 1234, b"bytes", [], {}]
    )
    def test_non_string_plaintext_returns_false(self, bad_plaintext):
        stored = ap.hash_password("validpw-12345", n=2, r=1, p=1)
        assert not ap.verify_password(bad_plaintext, stored)

    @pytest.mark.parametrize(
        "bad_stored", [None, 1234, b"bytes", [], {}]
    )
    def test_non_string_stored_returns_false(self, bad_stored):
        assert not ap.verify_password("anything", bad_stored)


# ------------------------------------------------------------------
# Override accessors
# ------------------------------------------------------------------

class TestOverrideAccessors:
    def test_initial_state(self):
        assert ap.get_admin_password_hash_override() is None

    def test_set_then_get(self):
        stored = ap.hash_password("setget-12345", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        assert ap.get_admin_password_hash_override() == stored

    def test_clear_returns_true_when_active(self):
        stored = ap.hash_password("clear-returns-1", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        assert ap.clear_admin_password_hash_override() is True
        assert ap.get_admin_password_hash_override() is None

    def test_clear_returns_false_when_not_active(self):
        assert ap.clear_admin_password_hash_override() is False

    def test_set_validates_format(self):
        with pytest.raises(ValueError):
            ap.set_admin_password_hash_override("not-a-scrypt-hash")
        with pytest.raises(ValueError):
            ap.set_admin_password_hash_override("bcrypt$10$abc$def$ghi$jkl")
        # Cache stays unchanged on failed set.
        assert ap.get_admin_password_hash_override() is None

    def test_set_rejects_non_str(self):
        with pytest.raises(TypeError):
            ap.set_admin_password_hash_override(b"scrypt$2$8$1$abc$def")
        with pytest.raises(TypeError):
            ap.set_admin_password_hash_override(None)


# ------------------------------------------------------------------
# DB refresh
# ------------------------------------------------------------------

class TestRefreshFromDB:
    @pytest.mark.asyncio
    async def test_none_db_keeps_cache(self):
        stored = ap.hash_password("none-db-12345", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        result = await ap.refresh_admin_password_hash_from_db(None)
        assert result == stored
        assert ap.get_admin_password_hash_override() == stored

    @pytest.mark.asyncio
    async def test_no_row_clears_cache(self):
        stored = ap.hash_password("no-row-1234567", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        db = type("FakeDB", (), {})()
        db.get_setting = AsyncMock(return_value=None)
        result = await ap.refresh_admin_password_hash_from_db(db)
        assert result is None
        assert ap.get_admin_password_hash_override() is None

    @pytest.mark.asyncio
    async def test_valid_row_loads_into_cache(self):
        stored = ap.hash_password("valid-row-1234", n=2, r=1, p=1)
        db = type("FakeDB", (), {})()
        db.get_setting = AsyncMock(return_value=stored)
        result = await ap.refresh_admin_password_hash_from_db(db)
        assert result == stored
        assert ap.get_admin_password_hash_override() == stored

    @pytest.mark.asyncio
    async def test_malformed_row_clears_cache(self):
        stored = ap.hash_password("malformed-row1", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        db = type("FakeDB", (), {})()
        db.get_setting = AsyncMock(return_value="not-a-real-hash")
        result = await ap.refresh_admin_password_hash_from_db(db)
        assert result is None
        assert ap.get_admin_password_hash_override() is None

    @pytest.mark.asyncio
    async def test_non_string_row_clears_cache(self):
        # A future schema bug or asyncpg quirk that returns bytes /
        # int / etc. should NOT poison the verify path.
        stored = ap.hash_password("nonstr-row1234", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        db = type("FakeDB", (), {})()
        db.get_setting = AsyncMock(return_value=12345)
        result = await ap.refresh_admin_password_hash_from_db(db)
        assert result is None
        assert ap.get_admin_password_hash_override() is None

    @pytest.mark.asyncio
    async def test_db_error_keeps_cache(self):
        stored = ap.hash_password("db-error-12345", n=2, r=1, p=1)
        ap.set_admin_password_hash_override(stored)
        db = type("FakeDB", (), {})()
        db.get_setting = AsyncMock(side_effect=RuntimeError("boom"))
        result = await ap.refresh_admin_password_hash_from_db(db)
        # Transient DB errors leave the in-process cache untouched —
        # an outage must not lock every operator out of the panel.
        assert result == stored
        assert ap.get_admin_password_hash_override() == stored


# ------------------------------------------------------------------
# Resolver
# ------------------------------------------------------------------

class TestVerifyAdminPassword:
    """``verify_admin_password`` resolves DB-hash → env-plaintext."""

    def test_no_credentials_rejects(self):
        assert not ap.verify_admin_password("anything")
        assert not ap.verify_admin_password("anything", env_expected="")

    def test_env_only_back_compat(self):
        assert ap.verify_admin_password(
            "letmein-1234", env_expected="letmein-1234",
        )
        assert not ap.verify_admin_password(
            "wrong-pass-1", env_expected="letmein-1234",
        )

    def test_db_hash_wins_over_env(self):
        # An operator who's rotated the password sees the OLD env
        # value rejected — only the new (DB-hashed) plaintext gets in.
        new_password = "NewPasswordRotated2024"
        ap.set_admin_password_hash_override(
            ap.hash_password(new_password, n=2, r=1, p=1)
        )
        assert ap.verify_admin_password(
            new_password, env_expected="OLD_ENV_VALUE_2023",
        )
        # Old env plaintext no longer gets in.
        assert not ap.verify_admin_password(
            "OLD_ENV_VALUE_2023", env_expected="OLD_ENV_VALUE_2023",
        )

    def test_clear_falls_back_to_env(self):
        ap.set_admin_password_hash_override(
            ap.hash_password("rotated-pw-12", n=2, r=1, p=1)
        )
        ap.clear_admin_password_hash_override()
        assert ap.verify_admin_password(
            "back-to-env-12", env_expected="back-to-env-12",
        )

    @pytest.mark.parametrize(
        "bad_plaintext", [None, 1234, b"bytes", [], {}]
    )
    def test_non_string_plaintext_rejects(self, bad_plaintext):
        assert not ap.verify_admin_password(
            bad_plaintext, env_expected="real-pw-12345",
        )

    def test_empty_plaintext_with_db_hash_rejects(self):
        # Edge: empty plaintext must NEVER be accepted. The bytes
        # compare against the scrypt digest of "" must yield False.
        ap.set_admin_password_hash_override(
            ap.hash_password("real-12345678", n=2, r=1, p=1)
        )
        assert not ap.verify_admin_password("")

    def test_empty_plaintext_with_env_rejects(self):
        # Even if ``env_expected`` were also "" — and at runtime the
        # main login path's ``if not expected`` guard refuses
        # whitespace-only env values up front — we still want this
        # path to refuse the empty plaintext explicitly so a future
        # caller bypassing that guard can't end up in a misconfig.
        # An empty env value treats both as "unset" and refuses.
        assert not ap.verify_admin_password("", env_expected="")


class TestGetAdminPasswordSource:
    def test_unset(self):
        assert ap.get_admin_password_source() == "unset"
        assert ap.get_admin_password_source(env_expected="") == "unset"

    def test_env(self):
        assert ap.get_admin_password_source(
            env_expected="some-env-value"
        ) == "env"

    def test_db_overrides_env(self):
        ap.set_admin_password_hash_override(
            ap.hash_password("source-12345", n=2, r=1, p=1)
        )
        assert ap.get_admin_password_source(
            env_expected="env-still-set"
        ) == "db"


class TestModuleConstants:
    def test_min_length_at_owasp_floor(self):
        assert ap.MIN_PASSWORD_LENGTH == 12

    def test_max_length_reasonable(self):
        # >=128 (so a long passphrase fits) but <= a few KB (DoS guard).
        assert 128 <= ap.MAX_PASSWORD_LENGTH <= 4096

    def test_setting_key_matches_env_name(self):
        # The SETTING_KEY must mirror the env-var name pattern so
        # docs / tooling can grep one canonical string.
        assert ap.ADMIN_PASSWORD_HASH_SETTING_KEY == "ADMIN_PASSWORD_HASH"

    def test_scrypt_n_is_power_of_two(self):
        n = ap.SCRYPT_N
        assert n >= 2 and (n & (n - 1)) == 0

    def test_scrypt_n_within_safety_bounds(self):
        # The hash-parse safety bounds MUST accommodate the default
        # so freshly-hashed passwords always verify.
        assert ap.SCRYPT_N <= ap.SCRYPT_N_MAX
        assert ap.SCRYPT_R <= ap.SCRYPT_R_MAX
        assert ap.SCRYPT_P <= ap.SCRYPT_P_MAX
