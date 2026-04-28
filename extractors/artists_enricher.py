"""
Enrichit les artistes Bronze avec les données complètes Spotify :
  popularity, followers.total, genres

Utilise l'endpoint batch :  GET /artists?ids=id1,id2,...  (max 50 par appel)
Lit les IDs depuis BigQuery (spotify_bronze.artists) et écrit les résultats
dans GCS sous  bronze/artist_enriched/  au format NDJSON.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

from auth.spotify_auth import auth_headers
from config import GCP_PROJECT_ID, GCS_BUCKET_NAME, GCS_BRONZE_PREFIX, SPOTIFY_MARKET
from extractors.base_extractor import BASE_URL, BaseExtractor

logger = logging.getLogger(__name__)

BATCH_SIZE = 50          # max autorisé par Spotify
GCS_PREFIX  = f"{GCS_BRONZE_PREFIX}/artist_enriched"
TABLE_ID    = f"{GCP_PROJECT_ID}.spotify_bronze.artist_enriched"


def _fetch_artist_ids() -> list[str]:
    """Retourne tous les IDs distincts depuis spotify_bronze.artists."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query  = f"SELECT DISTINCT id FROM `{GCP_PROJECT_ID}.spotify_bronze.artists` WHERE id IS NOT NULL"
    rows   = client.query(query).result()
    ids    = [row.id for row in rows]
    logger.info("IDs artistes récupérés depuis BigQuery : %d", len(ids))
    return ids


def _fetch_batch(ids: list[str]) -> list[dict]:
    """Appelle GET /artists?ids=... et retourne les objets enrichis."""
    # instancier un extractor temporaire juste pour utiliser _get et _enrich
    class _Dummy(BaseExtractor):
        resource_name = "artists"
        def extract(self): pass

    dummy = _Dummy()
    # Les virgules doivent rester NON encodées — on les intègre dans l'URL
    # (requests encode %2C si on passe ids= via params={})
    ids_str = ",".join(ids)
    url  = f"{BASE_URL}/artists?ids={ids_str}"
    data = dummy._get(url, params={})
    artists = data.get("artists", [])

    enriched = []
    for artist in artists:
        if artist:
            dummy._enrich(artist)   # ajoute _extraction_timestamp, _extraction_date, _market
            enriched.append(artist)

    time.sleep(0.3)   # délai poli
    return enriched


def _upload_to_gcs(records: list[dict], bucket_name: str) -> str:
    """Écrit les enregistrements NDJSON dans GCS (partitions hive)."""
    now    = datetime.now(timezone.utc)
    fname  = f"artist_enriched_{now.strftime('%Y%m%d_%H%M%S')}.ndjson"
    blob_path = (
        f"{GCS_PREFIX}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}"
        f"/{fname}"
    )

    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_path)
    blob.upload_from_string(ndjson, content_type="application/x-ndjson")

    logger.info("Fichier GCS écrit : gs://%s/%s (%d artistes)", bucket_name, blob_path, len(records))
    return f"gs://{bucket_name}/{blob_path}"


def _create_bq_table() -> None:
    """Crée (ou recrée) la table externe BigQuery pointant sur GCS."""
    client  = bigquery.Client(project=GCP_PROJECT_ID)
    dataset = bigquery.DatasetReference(GCP_PROJECT_ID, "spotify_bronze")

    ddl = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{TABLE_ID}`
    WITH PARTITION COLUMNS
    OPTIONS (
      format              = 'NEWLINE_DELIMITED_JSON',
      uris                = ['gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}/*'],
      hive_partition_uri_prefix = 'gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}',
      require_hive_partition_filter = false
    )
    """
    client.query(ddl).result()
    logger.info("Table BigQuery créée/mise à jour : %s", TABLE_ID)


def run_artists_enricher() -> int:
    """
    Charge tous les artistes Bronze, les enrichit via /artists?ids=...,
    écrit dans GCS et crée la table BigQuery.
    Retourne le nombre d'artistes enrichis.
    """
    ids     = _fetch_artist_ids()
    if not ids:
        logger.warning("Aucun ID artiste trouvé — pipeline non exécuté ?")
        return 0

    all_records: list[dict] = []
    total_batches = (len(ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(ids), BATCH_SIZE):
        batch  = ids[i: i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        logger.info("Batch %d/%d (%d artistes)", batch_num, total_batches, len(batch))
        records = _fetch_batch(batch)
        all_records.extend(records)

    if all_records:
        _upload_to_gcs(all_records, GCS_BUCKET_NAME)
        _create_bq_table()

    logger.info("Enrichissement terminé : %d artistes traités", len(all_records))
    return len(all_records)


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )
    run_artists_enricher()
