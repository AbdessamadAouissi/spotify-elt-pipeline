"""
Pipeline complet Spotify ELT — 6 phases pour maximiser les données.

Phase 1 : Recherche multi-passes (albums, artistes, tracks, playlists)
Phase 2 : SQL → Silver.artists (nécessaire pour la phase 3)
Phase 3 : Discographie complète par artiste (/artists/{id}/albums)
Phase 4 : SQL → Silver.albums (UNION search + discographie)
Phase 5 : Tous les titres par album (/albums/{id}/tracks)
Phase 6 : SQL → Silver.tracks + Gold (pipeline complet)
"""
import logging
import os
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("harvest")

PYTHON = sys.executable
ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Passes de recherche ───────────────────────────────────────────────────────
# Chaque passe utilise des queries différentes pour maximiser la couverture.
# La déduplication Silver élimine les doublons automatiquement.
# MAX_PAGES par passe — 20 pages × 10 items = 200 items max par ressource par passe
# Les playlists étaient à 250+ pages, ce qui déclenchait un rate limit de 10h
_MAX_PAGES_PER_PASS = "20"

SEARCH_PASSES = [
    {
        "label": "Rap FR 2020-2024",
        "SPOTIFY_SEARCH_QUERY":           "year:2020-2024",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rap francais",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rap francais",
        "MAX_PAGES": _MAX_PAGES_PER_PASS,
    },
    {
        "label": "Rap FR 2025-2026",
        "SPOTIFY_SEARCH_QUERY":           "year:2025-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rap francais 2025",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rap francais 2025",
        "MAX_PAGES": _MAX_PAGES_PER_PASS,
    },
    {
        "label": "Hip-hop FR",
        "SPOTIFY_SEARCH_QUERY":           "hip hop francais",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "hip hop francais",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "hip hop francais",
        "MAX_PAGES": _MAX_PAGES_PER_PASS,
    },
    {
        "label": "R&B FR",
        "SPOTIFY_SEARCH_QUERY":           "rnb francais",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "rnb francais",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "rnb francais",
        "MAX_PAGES": _MAX_PAGES_PER_PASS,
    },
    {
        "label": "Pop FR 2024-2026",
        "SPOTIFY_SEARCH_QUERY":           "year:2024-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "pop francaise",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "hits france 2025",
        "MAX_PAGES": _MAX_PAGES_PER_PASS,
    },
    {
        "label": "Top hits 2025-2026",
        "SPOTIFY_SEARCH_QUERY":           "year:2025-2026",
        "SPOTIFY_SEARCH_QUERY_ARTISTS":   "nouveautes 2026",
        "SPOTIFY_SEARCH_QUERY_PLAYLISTS": "nouveautes 2026",
        "MAX_PAGES": _MAX_PAGES_PER_PASS,
    },
]


def _run(cmd: list[str], label: str, env: dict = None) -> bool:
    """Exécute une commande et retourne True si succès."""
    logger.info(">>> %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        env=env or os.environ.copy(),
        cwd=ROOT,
    )
    if result.returncode != 0:
        logger.warning("[WARN] '%s' terminé avec code %d (continue)", label, result.returncode)
        return False
    return True


def phase1_search_passes() -> list[str]:
    """Phase 1 : Extraction via /search — plusieurs passes pour maximiser la couverture."""
    logger.info("\n%s", "=" * 65)
    logger.info("PHASE 1 — Recherche Spotify (%d passes)", len(SEARCH_PASSES))
    logger.info("=" * 65)

    failures = []
    for i, cfg in enumerate(SEARCH_PASSES, 1):
        label = cfg["label"]
        logger.info("Passe %d/%d — %s", i, len(SEARCH_PASSES), label)

        env = os.environ.copy()
        env.update({k: v for k, v in cfg.items() if k != "label"})

        ok = _run(
            [PYTHON, "pipeline.py", "--resources", "albums", "artists", "tracks", "playlists"],
            label=label,
            env=env,
        )
        if not ok:
            failures.append(label)

    return failures


def phase2_build_silver_artists() -> bool:
    """Phase 2 : Construit Silver.artists (requis par la phase 3)."""
    logger.info("\n%s", "=" * 65)
    logger.info("PHASE 2 — SQL → Silver.artists")
    logger.info("=" * 65)
    return _run(
        [PYTHON, "run_sql_pipeline.py", "--only", "bronze", "silver_artists"],
        label="Phase 2 SQL Silver.artists",
    )


def phase3_artist_albums() -> bool:
    """Phase 3 : Discographie complète de chaque artiste (/artists/{id}/albums)."""
    logger.info("\n%s", "=" * 65)
    logger.info("PHASE 3 — Discographie complète par artiste")
    logger.info("=" * 65)
    return _run(
        [PYTHON, "pipeline.py", "--resources", "artist_albums"],
        label="Phase 3 artist_albums",
    )


def phase4_build_silver_albums() -> bool:
    """Phase 4 : Crée la table Bronze artist_albums puis Silver.albums (UNION)."""
    logger.info("\n%s", "=" * 65)
    logger.info("PHASE 4 — SQL → Bronze.artist_albums + Silver.albums")
    logger.info("=" * 65)
    return _run(
        [PYTHON, "run_sql_pipeline.py", "--only", "artist_albums_table", "silver_albums"],
        label="Phase 4 SQL artist_albums + Silver.albums",
    )


def phase5_album_tracks() -> bool:
    """Phase 5 : Tous les titres de chaque album (/albums/{id}/tracks)."""
    logger.info("\n%s", "=" * 65)
    logger.info("PHASE 5 — Tous les titres par album")
    logger.info("=" * 65)
    return _run(
        [PYTHON, "pipeline.py", "--resources", "album_tracks"],
        label="Phase 5 album_tracks",
    )


def phase6_full_sql() -> bool:
    """Phase 6 : Pipeline SQL complet (Silver.tracks + playlists + Gold)."""
    logger.info("\n%s", "=" * 65)
    logger.info("PHASE 6 — SQL complet → Silver.tracks + Gold")
    logger.info("=" * 65)
    return _run(
        [PYTHON, "run_sql_pipeline.py"],
        label="Phase 6 SQL full",
    )


if __name__ == "__main__":
    search_failures = phase1_search_passes()
    phase2_build_silver_artists()
    phase3_artist_albums()
    phase4_build_silver_albums()
    phase5_album_tracks()
    ok_sql = phase6_full_sql()

    logger.info("\n%s", "=" * 65)
    logger.info("BILAN FINAL")
    logger.info("=" * 65)
    if search_failures:
        logger.warning("Passes avec avertissements : %s", ", ".join(search_failures))
    else:
        logger.info("Toutes les passes de recherche : OK")
    logger.info("SQL pipeline final : %s", "OK" if ok_sql else "ERREUR")

    if not ok_sql:
        sys.exit(1)
