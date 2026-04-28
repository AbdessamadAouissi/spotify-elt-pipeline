"""
create_looker_views.py
======================
Crée le dataset `spotify_looker` dans BigQuery et y déploie
les 6 vues optimisées pour Looker Studio.

Usage :
    python looker/create_looker_views.py

Prérequis :
    - Variable d'environnement GCP_PROJECT_ID définie
    - Credentials GCP (ADC) disponibles : gcloud auth application-default login
    - pip install google-cloud-bigquery
"""

import os
import sys
from pathlib import Path
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "smart-charter-301917")
LOOKER_DATASET = "spotify_looker"
REGION = "EU"

# ── SQL des vues ─────────────────────────────────────────────────────────────

VIEWS: list[dict] = [
    {
        "id": "lk_streams",
        "description": "Streams Spotify Charts enrichis — Page 1/2/3 Looker Studio",
        "sql": f"""
SELECT
  chart_date,
  chart_year,
  chart_month,
  year_month,
  FORMAT_DATE('%b %Y', CAST(chart_date AS DATE))          AS month_label,
  country,
  chart_type,
  rank,
  peak_rank,
  weeks_on_chart,
  track_id,
  track_name,
  artist_names,
  artist_name,
  duration_min,
  explicit,
  album_release_date,
  streams,
  cumulative_streams,
  ROUND(pct_monthly_streams, 4)                           AS pct_monthly_streams,
  ROUND(streams / 1000000.0, 2)                           AS streams_m,
  CASE
    WHEN rank = 1    THEN '#1'
    WHEN rank <= 3   THEN 'Top 3'
    WHEN rank <= 10  THEN 'Top 10'
    WHEN rank <= 50  THEN 'Top 50'
    ELSE 'Top 200'
  END                                                     AS rank_bucket
FROM `{PROJECT_ID}.spotify_gold.track_streams`
""",
    },
    {
        "id": "lk_artists",
        "description": "Métriques artistes — Page 2 Looker Studio",
        "sql": f"""
SELECT
  artist_id,
  artist_name,
  spotify_url,
  image_url,
  total_tracks,
  avg_track_duration_min,
  explicit_track_count,
  ROUND(explicit_track_count * 100.0 / NULLIF(total_tracks, 0), 1)  AS pct_explicit,
  total_albums,
  first_album_date,
  latest_album_date,
  career_span_years
FROM `{PROJECT_ID}.spotify_gold.top_artists`
""",
    },
    {
        "id": "lk_album_trends",
        "description": "Tendances de sorties d'albums — Page 4 Looker Studio",
        "sql": f"""
SELECT
  release_year,
  release_month,
  year_month,
  album_type,
  primary_artist_name,
  album_count,
  avg_tracks_per_album,
  max_tracks,
  min_tracks,
  cumulative_albums_in_year,
  pct_of_year_by_type
FROM `{PROJECT_ID}.spotify_gold.album_trends`
""",
    },
    {
        "id": "lk_release_patterns",
        "description": "Patterns de sortie (saisonnalité, vendredis) — Page 4",
        "sql": f"""
SELECT
  release_year,
  release_quarter,
  release_month,
  release_month_name,
  release_dow,
  release_day_name,
  album_type,
  primary_artist_name,
  releases_count,
  pct_of_year,
  friday_releases,
  pct_friday,
  avg_days_between_releases,
  artist_total_releases
FROM `{PROJECT_ID}.spotify_gold.release_patterns`
""",
    },
    {
        "id": "lk_playlist_insights",
        "description": "Segmentation playlists — Page 5 Looker Studio",
        "sql": f"""
SELECT
  size_bucket,
  is_public,
  is_collaborative,
  playlist_count,
  avg_tracks,
  max_tracks,
  min_tracks,
  median_tracks,
  unique_owners,
  top_owner_by_tracks,
  pct_of_total,
  CASE WHEN is_public THEN 'Publique' ELSE 'Privée' END   AS visibilite,
  CASE WHEN is_collaborative THEN 'Collaborative' ELSE 'Solo' END AS type_edition
FROM `{PROJECT_ID}.spotify_gold.playlist_insights`
""",
    },
    {
        "id": "lk_streams_weekly_summary",
        "description": "Résumé de la dernière semaine — scorecard Looker",
        "sql": f"""
WITH latest_week AS (
  SELECT MAX(chart_date) AS max_date
  FROM `{PROJECT_ID}.spotify_gold.track_streams`
)
SELECT
  s.chart_date,
  s.country,
  s.rank,
  s.track_name,
  s.artist_names,
  s.streams,
  s.peak_rank,
  s.weeks_on_chart,
  s.rank_bucket
FROM `{PROJECT_ID}.spotify_looker.lk_streams` s
CROSS JOIN latest_week lw
WHERE s.chart_date = lw.max_date
ORDER BY s.country, s.rank
""",
    },
]


# ── Fonctions ─────────────────────────────────────────────────────────────────

def ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{PROJECT_ID}.{LOOKER_DATASET}"
    try:
        client.get_dataset(dataset_ref)
        print(f"  [OK] Dataset {dataset_ref} existe déjà.")
    except Exception:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = REGION
        ds.description = "Vues optimisées pour Looker Studio — Spotify ELT"
        client.create_dataset(ds)
        print(f"  [CRÉÉ] Dataset {dataset_ref} ({REGION})")


def create_view(client: bigquery.Client, view: dict) -> None:
    view_id = f"{PROJECT_ID}.{LOOKER_DATASET}.{view['id']}"
    bq_view = bigquery.Table(view_id)
    bq_view.view_query = view["sql"].strip()
    bq_view.description = view["description"]

    try:
        client.delete_table(view_id, not_found_ok=True)
        client.create_table(bq_view)
        print(f"  [OK] Vue créée : {view['id']}")
    except Exception as e:
        print(f"  [ERREUR] {view['id']} : {e}", file=sys.stderr)
        raise


def main() -> None:
    print(f"\n{'='*60}")
    print(f"  Déploiement Looker Studio Views")
    print(f"  Projet : {PROJECT_ID}")
    print(f"  Dataset : {LOOKER_DATASET}")
    print(f"{'='*60}\n")

    client = bigquery.Client(project=PROJECT_ID)

    print("1. Vérification / création du dataset...")
    ensure_dataset(client)

    print(f"\n2. Création des {len(VIEWS)} vues...")
    for v in VIEWS:
        create_view(client, v)

    print(f"\n{'='*60}")
    print(f"  Terminé ! {len(VIEWS)} vues disponibles dans :")
    print(f"  {PROJECT_ID}.{LOOKER_DATASET}")
    print(f"\n  Dans Looker Studio, connecte-toi à :")
    for v in VIEWS:
        print(f"    → {v['id']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
