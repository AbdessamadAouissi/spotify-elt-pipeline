import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Generator

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

from auth.spotify_auth import auth_headers
from config import MAX_PAGES, PAGE_SIZE, SPOTIFY_MARKET

logger = logging.getLogger(__name__)

BASE_URL = "https://api.spotify.com/v1"


class RateLimitError(Exception):
    """Levée quand Spotify retourne HTTP 429, avec le délai Retry-After."""
    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__(f"Rate limit atteint — retry après {retry_after}s")


class BaseExtractor(ABC):
    """
    Classe de base pour tous les extracteurs Spotify.
    Gère : pagination, retry exponentiel, injection du timestamp d'extraction.
    """

    resource_name: str  # ex: "albums", "artists"

    @retry(
        retry=retry_if_exception_type((requests.HTTPError, RateLimitError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, url: str, params: dict = None) -> dict:
        resp = requests.get(url, headers=auth_headers(), params=params, timeout=15)
        if resp.status_code == 429:
            retry_after = min(int(resp.headers.get("Retry-After", 5)), 30)
            logger.warning("Rate limit atteint, attente %ds avant retry", retry_after)
            time.sleep(retry_after)
            raise RateLimitError(retry_after)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _normalize_dates(obj) -> None:
        """
        Normalise récursivement tous les champs 'release_date' en YYYY-MM-DD.
        Spotify peut retourner "1996" (année seule) ou "1996-03" (année-mois)
        qui sont invalides comme type DATE dans BigQuery.
        """
        if isinstance(obj, dict):
            if "release_date" in obj and isinstance(obj["release_date"], str):
                parts = obj["release_date"].split("-")
                if len(parts) == 1 and parts[0].isdigit():      # "1996"
                    obj["release_date"] = f"{parts[0]}-01-01"
                elif len(parts) == 2 and parts[1].isdigit():    # "1996-03"
                    obj["release_date"] = f"{parts[0]}-{parts[1]}-01"
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    BaseExtractor._normalize_dates(v)
        elif isinstance(obj, list):
            for item in obj:
                BaseExtractor._normalize_dates(item)

    def _enrich(self, record: dict) -> dict:
        """Ajoute les métadonnées d'extraction et normalise les dates."""
        self._normalize_dates(record)
        record["_extraction_timestamp"] = datetime.now(timezone.utc).isoformat()
        record["_extraction_date"] = datetime.now(timezone.utc).date().isoformat()
        record["_market"] = SPOTIFY_MARKET
        return record

    @abstractmethod
    def extract(self) -> Generator[dict, None, None]:
        """Yield des enregistrements bruts enrichis un par un."""
        ...

    def paginate(self, url: str, params: dict) -> Generator[dict, None, None]:
        """Itère sur toutes les pages d'un endpoint de recherche Spotify."""
        params = {**params, "limit": PAGE_SIZE, "offset": 0}
        page = 0

        while page < MAX_PAGES:
            time.sleep(0.3)   # délai poli entre requêtes pour éviter le rate limit
            data = self._get(url, params)
            items_wrapper = data.get(self.resource_name, data)
            items = items_wrapper.get("items", [])

            if not items:
                break

            logger.info(
                "[%s] Page %d — %d items (offset %d)",
                self.resource_name, page, len(items), params["offset"],
            )

            for item in items:
                if item:
                    yield self._enrich(item)

            next_url = items_wrapper.get("next")
            if not next_url:
                break

            params["offset"] += PAGE_SIZE
            page += 1
