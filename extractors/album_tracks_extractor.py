"""
Extrait tous les tracks depuis les albums déjà présents en Silver.
Appelle /albums/{id}/tracks pour chaque album → données complètes et fiables.
"""
import logging
import time
from typing import Generator

from google.cloud import bigquery

from config import GCP_PROJECT_ID, BIGQUERY_DATASET_SILVER, SPOTIFY_MARKET
from extractors.base_extractor import BASE_URL, BaseExtractor

logger = logging.getLogger(__name__)

# Délai entre albums pour éviter le rate limiting (429)
_ALBUM_DELAY_S = 0.5


class AlbumTracksExtractor(BaseExtractor):
    resource_name = "tracks"

    def _get_album_ids(self) -> list[dict]:
        """Récupère id + métadonnées des albums dédupliqués depuis Silver (stg_albums)."""
        client = bigquery.Client(project=GCP_PROJECT_ID)
        rows = client.query(f"""
            SELECT id, name, release_date, album_type, primary_artist_id, primary_artist_name
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_SILVER}.stg_albums`
            ORDER BY release_date DESC
            LIMIT 200
        """).result()
        return [dict(r) for r in rows]

    def extract(self) -> Generator[dict, None, None]:
        albums = self._get_album_ids()
        logger.info("AlbumTracksExtractor: %d albums à traiter", len(albums))

        skipped = 0
        for i, album in enumerate(albums):
            if i > 0:
                time.sleep(_ALBUM_DELAY_S)

            album_id = album["id"]
            url = f"{BASE_URL}/albums/{album_id}/tracks"
            params = {"limit": 50, "market": SPOTIFY_MARKET, "offset": 0}
            total = 0

            while True:
                try:
                    data = self._get(url, params)
                except Exception as exc:
                    logger.warning(
                        "  [SKIP] %s (%s) — %s",
                        album["name"][:40], album_id[:8], exc,
                    )
                    skipped += 1
                    break

                items = data.get("items", [])
                if not items:
                    break

                for track in items:
                    if not track:
                        continue
                    # Injecter les métadonnées album dans chaque track
                    track["album"] = {
                        "id":           album["id"],
                        "name":         album["name"],
                        "release_date": str(album["release_date"]) if album["release_date"] else None,
                        "album_type":   album["album_type"],
                        "artists": [{"id": album["primary_artist_id"], "name": album["primary_artist_name"]}],
                    }
                    yield self._enrich(track)
                    total += 1

                if data.get("next"):
                    params["offset"] += 50
                else:
                    break

            logger.info(
                "  [%d/%d] %s — %d tracks",
                i + 1, len(albums), album["name"][:40], total,
            )

        if skipped:
            logger.warning("AlbumTracksExtractor: %d albums ignorés (erreur API)", skipped)
