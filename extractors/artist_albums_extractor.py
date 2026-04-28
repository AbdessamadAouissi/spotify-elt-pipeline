"""
Pour chaque artiste présent dans Silver, récupère TOUS ses albums
via /artists/{id}/albums (albums, singles, compilations).

Permet d'obtenir la discographie complète plutôt que seulement
ce que retourne /search.
"""
import logging
import time
from typing import Generator

from google.cloud import bigquery

from config import GCP_PROJECT_ID, BIGQUERY_DATASET_SILVER, SPOTIFY_MARKET
from extractors.base_extractor import BASE_URL, BaseExtractor

logger = logging.getLogger(__name__)

# Délai entre chaque artiste pour éviter le rate limiting (429)
_ARTIST_DELAY_S = 1.2


class ArtistAlbumsExtractor(BaseExtractor):
    resource_name = "artist_albums"

    def _get_artists(self) -> list[dict]:
        """
        Récupère les artistes dédupliqués depuis Silver.
        Limite à MAX_ARTISTS pour éviter des runs de plusieurs heures.
        """
        client = bigquery.Client(project=GCP_PROJECT_ID)
        rows = client.query(f"""
            SELECT DISTINCT id, name
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_SILVER}.stg_artists`
            ORDER BY name
            LIMIT 100
        """).result()
        return [dict(r) for r in rows]

    def extract(self) -> Generator[dict, None, None]:
        artists = self._get_artists()
        logger.info("ArtistAlbumsExtractor: %d artistes à traiter", len(artists))

        skipped = 0
        for i, artist in enumerate(artists):
            # Délai poli entre chaque artiste
            if i > 0:
                time.sleep(_ARTIST_DELAY_S)

            # include_groups avec virgules littérales dans l'URL
            # (requests.get() avec params= encoderait les virgules en %2C → 400)
            base_url = f"{BASE_URL}/artists/{artist['id']}/albums"

            total = 0
            offset = 0
            while True:
                url = (
                    f"{base_url}?include_groups=album,single,compilation"
                    f"&market={SPOTIFY_MARKET}&limit=50&offset={offset}"
                )
                try:
                    data = self._get(url)
                except Exception as exc:
                    logger.warning(
                        "  [SKIP] %s (%s) — %s",
                        artist["name"][:40], artist["id"][:8], exc,
                    )
                    skipped += 1
                    break

                items = data.get("items", [])
                if not items:
                    break

                for album in items:
                    if not album:
                        continue
                    yield self._enrich(album)
                    total += 1

                if data.get("next"):
                    offset += 50
                else:
                    break

            logger.info(
                "  [%d/%d] %s — %d albums",
                i + 1, len(artists), artist["name"][:40], total,
            )

        if skipped:
            logger.warning("ArtistAlbumsExtractor: %d artistes ignorés (erreur API)", skipped)
