"""Tests unitaires — loaders/gcs_loader.py"""
import re
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from loaders.gcs_loader import bronze_gcs_path


def test_path_format():
    path = bronze_gcs_path("artists")
    # Doit matcher : bronze/artists/year=YYYY/month=MM/day=DD/artists_YYYYMMDD_HHMMSS.ndjson
    pattern = r"^bronze/artists/year=\d{4}/month=\d{2}/day=\d{2}/artists_\d{8}_\d{6}\.ndjson$"
    assert re.match(pattern, path), f"Path inattendu : {path}"


def test_path_contains_resource_name():
    for resource in ["albums", "artists", "tracks", "playlists", "album_tracks"]:
        path = bronze_gcs_path(resource)
        assert resource in path


def test_path_uses_utc():
    fixed_dt = datetime(2025, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
    with patch("loaders.gcs_loader.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_dt
        path = bronze_gcs_path("tracks")

    assert "year=2025" in path
    assert "month=06" in path
    assert "day=15" in path
    assert "tracks_20250615_123045.ndjson" in path


def test_path_prefix_from_config():
    path = bronze_gcs_path("albums")
    assert path.startswith("bronze/")


def test_different_resources_have_different_paths():
    path_albums = bronze_gcs_path("albums")
    path_artists = bronze_gcs_path("artists")
    assert path_albums != path_artists
    assert "albums" in path_albums
    assert "artists" in path_artists
