"""
Charge les fichiers CSV Spotify Charts dans BigQuery.

Comment télécharger les charts :
  1. Aller sur https://charts.spotify.com/charts/overview/fr
  2. Se connecter avec son compte Spotify
  3. Sélectionner la période (semaine) et le pays (FR)
  4. Cliquer sur "Download CSV" en haut à droite
  5. Placer les fichiers dans le dossier charts/ à la racine du projet

Format attendu du fichier :  regional-fr-weekly-YYYY-MM-DD--YYYY-MM-DD.csv
Colonnes CSV :               rank, uri, artist_names, track_name, source,
                             peak_rank, previous_rank, weeks_on_chart, streams
"""

import csv
import logging
import re
from datetime import date, datetime
from pathlib import Path

from google.cloud import bigquery

from config import GCP_PROJECT_ID, BIGQUERY_DATASET_SILVER

logger = logging.getLogger(__name__)

CHARTS_DIR = Path(__file__).resolve().parent.parent / "charts"
TABLE_ID = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_SILVER}.spotify_charts"

SCHEMA = [
    bigquery.SchemaField("chart_date",       "DATE"),
    bigquery.SchemaField("country",          "STRING"),
    bigquery.SchemaField("chart_type",       "STRING"),    # weekly / daily
    bigquery.SchemaField("rank",             "INTEGER"),
    bigquery.SchemaField("track_uri",        "STRING"),
    bigquery.SchemaField("track_id",         "STRING"),    # extrait du URI
    bigquery.SchemaField("track_name",       "STRING"),
    bigquery.SchemaField("artist_names",     "STRING"),
    bigquery.SchemaField("streams",          "INTEGER"),
    bigquery.SchemaField("peak_rank",        "INTEGER"),
    bigquery.SchemaField("previous_rank",    "INTEGER"),
    bigquery.SchemaField("weeks_on_chart",   "INTEGER"),
    bigquery.SchemaField("source",           "STRING"),
]

# Regex pour parser le nom de fichier Spotify Charts
# Formats supportés :
#   regional-fr-weekly-2026-01-08.csv                      (nouveau format)
#   regional-fr-weekly-2024-01-04--2024-01-11.csv          (ancien format)
_FNAME_RE = re.compile(
    r"(?P<type>regional|viral)-(?P<country>[a-z]+)-(?P<freq>weekly|daily)"
    r"-(?P<start>\d{4}-\d{2}-\d{2})(?:--(?P<end>\d{4}-\d{2}-\d{2}))?\.csv",
    re.IGNORECASE,
)


def _parse_filename(path: Path) -> dict | None:
    m = _FNAME_RE.match(path.name)
    if not m:
        logger.warning("Nom de fichier non reconnu, ignoré : %s", path.name)
        return None
    return {
        "chart_type": m.group("freq"),
        "country":    m.group("country").upper(),
        "chart_date": m.group("start"),   # date de début de la semaine
    }


def _extract_track_id(uri: str) -> str:
    """spotify:track:4iV5W9uYEdYUVa79Axb7Rh → 4iV5W9uYEdYUVa79Axb7Rh"""
    return uri.split(":")[-1] if uri else ""


def _safe_int(val: str) -> int | None:
    try:
        return int(val.replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def load_charts_csv(csv_path: Path) -> list[dict]:
    """Lit un fichier CSV Spotify Charts et retourne une liste de dicts."""
    meta = _parse_filename(csv_path)
    if not meta:
        return []

    rows = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        # Spotify Charts CSV has a header comment line starting with "#"
        lines = [l for l in f.readlines() if not l.startswith("#")]

    reader = csv.DictReader(lines)
    for row in reader:
        uri = row.get("uri", "").strip()
        rows.append({
            "chart_date":      meta["chart_date"],
            "country":         meta["country"],
            "chart_type":      meta["chart_type"],
            "rank":            _safe_int(row.get("rank", "")),
            "track_uri":       uri,
            "track_id":        _extract_track_id(uri),
            "track_name":      row.get("track_name", "").strip(),
            "artist_names":    row.get("artist_names", "").strip(),
            "streams":         _safe_int(row.get("streams", "")),
            "peak_rank":       _safe_int(row.get("peak_rank", "")),
            "previous_rank":   _safe_int(row.get("previous_rank", "")),
            "weeks_on_chart":  _safe_int(row.get("weeks_on_chart", "")),
            "source":          row.get("source", "").strip(),
        })

    logger.info("  %s → %d lignes (%s %s)", csv_path.name, len(rows), meta["country"], meta["chart_date"])
    return rows


def upload_to_bigquery(rows: list[dict]) -> None:
    if not rows:
        return

    client = bigquery.Client(project=GCP_PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # Crée la table si elle n'existe pas
    table_ref = bigquery.Table(TABLE_ID, schema=SCHEMA)
    client.create_table(table_ref, exists_ok=True)

    # Charge les lignes
    errors = client.insert_rows_json(TABLE_ID, rows)
    if errors:
        logger.error("Erreurs d'insertion BigQuery : %s", errors)
    else:
        logger.info("  → %d lignes insérées dans %s", len(rows), TABLE_ID)


def run_charts_loader(charts_dir: Path = CHARTS_DIR) -> int:
    """
    Charge tous les CSV du dossier charts/ dans BigQuery.
    Retourne le nombre total de lignes chargées.
    """
    if not charts_dir.exists():
        logger.error("Dossier '%s' introuvable — créez-le et placez-y les CSV Spotify Charts.", charts_dir)
        return 0

    csv_files = sorted(charts_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("Aucun fichier CSV dans '%s'.", charts_dir)
        return 0

    logger.info("Charts loader : %d fichiers trouvés dans %s", len(csv_files), charts_dir)

    total = 0
    for path in csv_files:
        rows = load_charts_csv(path)
        if rows:
            upload_to_bigquery(rows)
            total += len(rows)

    logger.info("Charts loader terminé : %d lignes chargées au total", total)
    return total


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )
    run_charts_loader()
