import base64
import logging
import time

import requests

from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

logger = logging.getLogger(__name__)

TOKEN_URL = "https://accounts.spotify.com/api/token"

_cache: dict = {"token": None, "expires_at": 0.0}


def _fetch_token() -> str:
    credentials = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
    ).decode()

    resp = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {credentials}"},
        data={"grant_type": "client_credentials"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    logger.info("Nouveau token Spotify obtenu (expire dans %ds)", data["expires_in"])
    return data["access_token"], data["expires_in"]


def get_token() -> str:
    """Retourne un token valide, re-fetche si expiré (avec 60s de marge)."""
    if time.monotonic() < _cache["expires_at"] - 60:
        return _cache["token"]

    token, ttl = _fetch_token()
    _cache["token"] = token
    _cache["expires_at"] = time.monotonic() + ttl
    return token


def auth_headers() -> dict:
    return {"Authorization": f"Bearer {get_token()}"}
