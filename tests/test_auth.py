"""Tests unitaires — auth/spotify_auth.py"""
import time
from unittest.mock import MagicMock, patch

import pytest

import auth.spotify_auth as spotify_auth


@pytest.fixture(autouse=True)
def reset_cache():
    """Réinitialise le cache token avant chaque test."""
    spotify_auth._cache["token"] = None
    spotify_auth._cache["expires_at"] = 0.0
    yield
    spotify_auth._cache["token"] = None
    spotify_auth._cache["expires_at"] = 0.0


def _mock_fetch(token="test_token", ttl=3600):
    return patch(
        "auth.spotify_auth._fetch_token",
        return_value=(token, ttl),
    )


def test_get_token_calls_fetch_when_cache_empty():
    with _mock_fetch("fresh_token") as mock_fetch:
        token = spotify_auth.get_token()
    mock_fetch.assert_called_once()
    assert token == "fresh_token"


def test_get_token_uses_cache_when_valid():
    spotify_auth._cache["token"] = "cached_token"
    spotify_auth._cache["expires_at"] = time.monotonic() + 3600  # expire dans 1h

    with _mock_fetch("new_token") as mock_fetch:
        token = spotify_auth.get_token()

    mock_fetch.assert_not_called()
    assert token == "cached_token"


def test_get_token_refreshes_when_expired():
    spotify_auth._cache["token"] = "old_token"
    spotify_auth._cache["expires_at"] = time.monotonic() - 1  # expiré

    with _mock_fetch("refreshed_token") as mock_fetch:
        token = spotify_auth.get_token()

    mock_fetch.assert_called_once()
    assert token == "refreshed_token"


def test_get_token_refreshes_within_60s_margin():
    """Le token doit être rafraîchi si < 60s restants."""
    spotify_auth._cache["token"] = "expiring_soon"
    spotify_auth._cache["expires_at"] = time.monotonic() + 30  # 30s restantes < marge 60s

    with _mock_fetch("new_token") as mock_fetch:
        token = spotify_auth.get_token()

    mock_fetch.assert_called_once()
    assert token == "new_token"


def test_auth_headers_format():
    spotify_auth._cache["token"] = "mytoken123"
    spotify_auth._cache["expires_at"] = time.monotonic() + 3600

    headers = spotify_auth.auth_headers()
    assert headers == {"Authorization": "Bearer mytoken123"}


def test_cache_is_populated_after_fetch():
    with _mock_fetch("stored_token", ttl=1800):
        spotify_auth.get_token()

    assert spotify_auth._cache["token"] == "stored_token"
    assert spotify_auth._cache["expires_at"] > time.monotonic()
