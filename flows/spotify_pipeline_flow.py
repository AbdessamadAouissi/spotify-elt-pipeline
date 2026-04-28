"""
Prefect orchestration flow — Spotify ELT Pipeline.

Séquence complète :
  1. Passes /search        → bronze (albums, artistes, tracks, playlists)
  2. dbt Silver.artists
  3. /artists/{id}/albums  → bronze/artist_albums/
  4. dbt Silver.albums     (UNION search + discographie)
  5. /albums/{id}/tracks   → bronze/album_tracks/
  6. charts_loader.py      → silver.spotify_charts (CSV Spotify Charts)
  7. dbt Silver.tracks + Gold (dont track_streams)
  8. dbt test

Usage :
  python flows/spotify_pipeline_flow.py          # run local
  prefect deploy --all                            # déployer le schedule
"""

import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, task, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── Passes de recherche ───────────────────────────────────────────────────────
SEARCH_PASSES = [
    {
        "label": "Rap FR 2020-2024",
        "SPOTIFY_SEARCH_QUERY":           "year:2020-2024",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rap francais",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rap francais",
    },
    {
        "label": "Rap FR 2025-2026",
        "SPOTIFY_SEARCH_QUERY":           "year:2025-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rap francais 2025",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rap francais 2025",
    },
    {
        "label": "Hip-hop FR",
        "SPOTIFY_SEARCH_QUERY":           "hip hop francais",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "hip hop francais",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "hip hop francais",
    },
    {
        "label": "R&B FR",
        "SPOTIFY_SEARCH_QUERY":           "rnb francais",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rnb francais",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rnb francais",
    },
    {
        "label": "Pop FR 2024-2026",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "pop francaise",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "hits france 2025",
    },
    {
        "label": "Top hits 2025-2026",
        "SPOTIFY_SEARCH_QUERY":           "year:2025-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "nouveautes 2026",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "nouveautes 2026",
    },
    # ── Europe — élargissement multi-pays ────────────────────────────────────
    {
        "label": "Pop DE",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "deutsch pop",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "top hits germany 2025",
    },
    {
        "label": "Rap DE",
        "SPOTIFY_SEARCH_QUERY":           "deutsch rap",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "deutsch rap",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "deutschrap 2025",
    },
    {
        "label": "Pop ES",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "pop espanol",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "exitos espana 2025",
    },
    {
        "label": "Reggaeton ES",
        "SPOTIFY_SEARCH_QUERY":           "reggaeton",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "reggaeton espana",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "reggaeton 2025",
    },
    {
        "label": "Pop IT",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "musica italiana",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "top italia 2025",
    },
    {
        "label": "Rap IT",
        "SPOTIFY_SEARCH_QUERY":           "rap italiano",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rap italiano",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rap italiano 2025",
    },
    {
        "label": "Pop PT/BR",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "musica portuguesa",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "top portugal 2025",
    },
    {
        "label": "Pop NL/BE",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "nederlandse pop",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "top hits nederland 2025",
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_subprocess(cmd: list[str], env: dict = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        env=env or {**os.environ},
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )


def _log_result(logger, result: subprocess.CompletedProcess, label: str) -> None:
    if result.stdout:
        logger.info(result.stdout[-4000:])
    if result.stderr:
        logger.warning(result.stderr[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"'{label}' failed (exit {result.returncode})")


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="search-pass", retries=3, retry_delay_seconds=60)
def search_pass(label: str, env_overrides: dict) -> None:
    logger = get_run_logger()
    logger.info("Search pass: %s", label)
    env = {**os.environ, **env_overrides}
    result = _run_subprocess(
        [sys.executable, "pipeline.py", "--resources", "albums", "artists", "tracks", "playlists"],
        env=env,
    )
    _log_result(logger, result, label)


@task(name="extract-artist-albums", retries=3, retry_delay_seconds=60)
def extract_artist_albums() -> None:
    """Discographie complète par artiste — nécessite Silver.artists."""
    logger = get_run_logger()
    logger.info("Extracting artist albums (/artists/{id}/albums) …")
    result = _run_subprocess([sys.executable, "pipeline.py", "--resources", "artist_albums"])
    _log_result(logger, result, "artist_albums")


@task(name="extract-album-tracks", retries=3, retry_delay_seconds=60)
def extract_album_tracks() -> None:
    """Tous les titres par album — nécessite Silver.albums."""
    logger = get_run_logger()
    logger.info("Extracting album tracks (/albums/{id}/tracks) …")
    result = _run_subprocess([sys.executable, "pipeline.py", "--resources", "album_tracks"])
    _log_result(logger, result, "album_tracks")


@task(name="load-spotify-charts", retries=2, retry_delay_seconds=30)
def load_spotify_charts() -> None:
    """Charge les CSV Spotify Charts présents dans charts/ vers BigQuery Silver."""
    logger = get_run_logger()
    charts_dir = PROJECT_ROOT / "charts"
    csvs = list(charts_dir.glob("regional-*-weekly-*.csv"))
    if not csvs:
        logger.warning("Aucun CSV trouvé dans %s — skip.", charts_dir)
        return
    logger.info("Loading %d Spotify Charts CSV(s) from %s", len(csvs), charts_dir)
    result = _run_subprocess([
        sys.executable, "loaders/charts_loader.py", "--charts-dir", str(charts_dir),
    ])
    _log_result(logger, result, "charts_loader")


@task(name="dbt-run", retries=2, retry_delay_seconds=30)
def dbt_run(select: str = "") -> None:
    logger = get_run_logger()
    cmd = [
        "dbt", "run",
        "--project-dir", str(PROJECT_ROOT / "dbt"),
        "--profiles-dir", str(PROJECT_ROOT / "dbt"),
    ]
    if select:
        cmd += ["--select", select]
    logger.info("Running: %s", " ".join(cmd))
    result = _run_subprocess(cmd)
    _log_result(logger, result, f"dbt run {select or '(all)'}")


@task(name="dbt-test", retries=1, retry_delay_seconds=15)
def dbt_test() -> None:
    logger = get_run_logger()
    cmd = [
        "dbt", "test",
        "--project-dir", str(PROJECT_ROOT / "dbt"),
        "--profiles-dir", str(PROJECT_ROOT / "dbt"),
    ]
    result = _run_subprocess(cmd)
    _log_result(logger, result, "dbt test")


# ── Main flow ─────────────────────────────────────────────────────────────────

@flow(
    name="spotify-pipeline",
    description=(
        "Spotify ELT — search → artist_albums → album_tracks → dbt Silver + Gold"
    ),
    log_prints=True,
)
def spotify_pipeline(run_tests: bool = True) -> None:
    logger = get_run_logger()
    logger.info("=== Spotify ELT Pipeline started ===")

    # Phase 1 : Recherche (séquentielle — rate limits API)
    for cfg in SEARCH_PASSES:
        label = cfg["label"]
        overrides = {k: v for k, v in cfg.items() if k != "label"}
        search_pass(label=label, env_overrides=overrides)

    # Phase 2 : Silver.artists (base pour artist_albums)
    dbt_run(select="stg_artists")

    # Phase 3 : Discographie complète
    extract_artist_albums()

    # Phase 4 : Silver.albums (UNION search + discographie)
    dbt_run(select="stg_albums")

    # Phase 5 : Tous les titres par album
    extract_album_tracks()

    # Phase 6 : Charger les CSV Spotify Charts (streams hebdomadaires)
    load_spotify_charts()

    # Phase 7 : Silver complet (stg_tracks, stg_playlists) + Gold (dont track_streams)
    dbt_run()

    # Phase 8 : Tests qualité
    if run_tests:
        dbt_test()

    logger.info("=== Spotify ELT Pipeline completed ===")


if __name__ == "__main__":
    spotify_pipeline()
