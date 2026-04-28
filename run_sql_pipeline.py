"""
Exécute les SQL Bronze → Silver → Gold dans BigQuery.
Remplace automatiquement les placeholders depuis config.py.
"""

import logging
import sys
from pathlib import Path

from google.cloud import bigquery

from config import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    BIGQUERY_DATASET_BRONZE,
    BIGQUERY_DATASET_SILVER,
    BIGQUERY_DATASET_GOLD,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("sql_pipeline")

PLACEHOLDERS = {
    "YOUR_PROJECT":        GCP_PROJECT_ID,
    "YOUR_BUCKET":         GCS_BUCKET_NAME,
    "YOUR_BRONZE_DATASET": BIGQUERY_DATASET_BRONZE,
    "YOUR_SILVER_DATASET": BIGQUERY_DATASET_SILVER,
    "YOUR_GOLD_DATASET":   BIGQUERY_DATASET_GOLD,
}

SQL_ORDER = [
    "sql/bronze/create_external_tables.sql",
    "sql/bronze/create_artist_albums_table.sql",  # créée après extraction artist_albums
    "sql/silver/silver_artists.sql",
    "sql/silver/silver_albums.sql",
    "sql/silver/silver_tracks.sql",
    "sql/silver/silver_playlists.sql",
    "sql/gold/gold_top_artists.sql",
    "sql/gold/gold_album_trends.sql",
    "sql/gold/gold_release_patterns.sql",
    "sql/gold/gold_playlist_insights.sql",
]


def replace_placeholders(sql: str) -> str:
    for placeholder, value in PLACEHOLDERS.items():
        sql = sql.replace(placeholder, value)
    return sql


def run_sql_file(client: bigquery.Client, sql_path: str) -> bool:
    path = Path(sql_path)
    raw_sql = path.read_text(encoding="utf-8")
    sql = replace_placeholders(raw_sql)

    # Séparer les statements par ";" et garder ceux qui contiennent du SQL réel
    def has_sql(s: str) -> bool:
        keywords = ("CREATE", "INSERT", "SELECT", "DROP", "ALTER", "MERGE")
        return any(kw in s.upper() for kw in keywords)

    statements = [s.strip() for s in sql.split(";") if s.strip() and has_sql(s)]

    logger.info("=== %s (%d statements) ===", path.name, len(statements))

    for i, stmt in enumerate(statements, 1):
        try:
            job = client.query(stmt)
            job.result()  # attend la fin
            logger.info("  [%d/%d] OK — %s", i, len(statements), stmt[:80].replace("\n", " "))
        except Exception as exc:
            logger.error("  [%d/%d] ERREUR : %s", i, len(statements), exc)
            return False

    return True


def main(only: list[str] = None):
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Filtrer les fichiers SQL si --only est fourni
    files_to_run = SQL_ORDER
    if only:
        files_to_run = [f for f in SQL_ORDER if any(pat in f for pat in only)]
        logger.info("Filtrage --only %s → %d fichiers", only, len(files_to_run))

    errors = []
    for sql_file in files_to_run:
        ok = run_sql_file(client, sql_file)
        if not ok:
            errors.append(sql_file)

    logger.info("\n%s", "=" * 60)
    logger.info("RÉSUMÉ SQL PIPELINE")
    logger.info("=" * 60)
    for f in files_to_run:
        status = "ERREUR" if f in errors else "OK"
        logger.info("[%s] %s", status, f)

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SQL pipeline BigQuery Bronze → Silver → Gold")
    parser.add_argument(
        "--only",
        nargs="+",
        default=None,
        help="N'exécuter que les fichiers SQL dont le nom contient ces motifs (ex: bronze silver_artists)",
    )
    args = parser.parse_args()
    main(only=args.only)
