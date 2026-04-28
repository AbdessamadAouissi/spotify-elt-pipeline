"""
Point d'entrée du pipeline ELT Spotify → GCS (Bronze).

Flux : Spotify API → Python Extractor → NDJSON Transformer → GCS Loader
"""

import logging
import sys
from dataclasses import dataclass
from typing import Type

from extractors.album_tracks_extractor import AlbumTracksExtractor
from extractors.albums_extractor import AlbumsExtractor
from extractors.artist_albums_extractor import ArtistAlbumsExtractor
from extractors.artists_extractor import ArtistsExtractor
from extractors.base_extractor import BaseExtractor
from extractors.playlists_extractor import PlaylistsExtractor
from extractors.tracks_extractor import TracksExtractor
from loaders.gcs_loader import upload_ndjson
from transformers.ndjson_transformer import to_ndjson

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("pipeline")


@dataclass
class PipelineResult:
    resource: str
    record_count: int
    gcs_uri: str
    success: bool
    error: str = None


EXTRACTORS: dict[str, Type[BaseExtractor]] = {
    "albums": AlbumsExtractor,
    "artists": ArtistsExtractor,
    "tracks": TracksExtractor,
    "artist_albums": ArtistAlbumsExtractor,  # discographie complète par artiste (Silver requis)
    "album_tracks": AlbumTracksExtractor,    # tous les titres par album (Silver requis)
    "playlists": PlaylistsExtractor,
}


def run_resource(resource: str) -> PipelineResult:
    logger.info("=== Démarrage extraction : %s ===", resource)
    try:
        extractor = EXTRACTORS[resource]()
        records = extractor.extract()
        buf, count = to_ndjson(records)
        uri = upload_ndjson(buf, resource)
        logger.info("=== %s terminé : %d records → %s ===", resource, count, uri)
        return PipelineResult(resource=resource, record_count=count, gcs_uri=uri, success=True)
    except Exception as exc:
        logger.exception("Erreur sur resource '%s' : %s", resource, exc)
        return PipelineResult(resource=resource, record_count=0, gcs_uri="", success=False, error=str(exc))


def run_pipeline(resources: list[str] = None) -> list[PipelineResult]:
    targets = resources or list(EXTRACTORS.keys())
    results = [run_resource(r) for r in targets]

    logger.info("\n%s", "=" * 60)
    logger.info("RÉSUMÉ DU PIPELINE")
    logger.info("=" * 60)
    for r in results:
        status = "OK" if r.success else "ERREUR"
        logger.info("[%s] %-12s — %d records — %s", status, r.resource, r.record_count, r.gcs_uri or r.error)

    failed = [r for r in results if not r.success]
    if failed:
        sys.exit(1)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline ELT Spotify → GCS")
    parser.add_argument(
        "--resources",
        nargs="+",
        choices=list(EXTRACTORS.keys()),
        default=None,
        help="Resources à extraire (défaut: toutes)",
    )
    args = parser.parse_args()
    run_pipeline(args.resources)
