"""Tests unitaires — config.py (validation des variables d'environnement)"""
import importlib
import os
import sys
from unittest.mock import patch

import pytest


def reload_config(env_overrides: dict):
    """Recharge config.py avec un environnement modifié."""
    env = {
        "SPOTIFY_CLIENT_ID": "test_id",
        "SPOTIFY_CLIENT_SECRET": "test_secret",
        "GCP_PROJECT_ID": "test_project",
        "GCS_BUCKET_NAME": "test_bucket",
        **env_overrides,
    }
    with patch.dict(os.environ, env, clear=True):
        if "config" in sys.modules:
            del sys.modules["config"]
        import config
        return config


def test_config_loads_with_all_required_vars():
    cfg = reload_config({})
    assert cfg.SPOTIFY_CLIENT_ID == "test_id"
    assert cfg.SPOTIFY_CLIENT_SECRET == "test_secret"
    assert cfg.GCP_PROJECT_ID == "test_project"
    assert cfg.GCS_BUCKET_NAME == "test_bucket"


def test_config_raises_on_missing_client_id():
    with pytest.raises(EnvironmentError, match="SPOTIFY_CLIENT_ID"):
        reload_config({"SPOTIFY_CLIENT_ID": ""})


def test_config_raises_on_missing_client_secret():
    with pytest.raises(EnvironmentError, match="SPOTIFY_CLIENT_SECRET"):
        reload_config({"SPOTIFY_CLIENT_SECRET": ""})


def test_config_raises_on_missing_gcp_project():
    with pytest.raises(EnvironmentError, match="GCP_PROJECT_ID"):
        reload_config({"GCP_PROJECT_ID": ""})


def test_config_raises_on_missing_bucket():
    with pytest.raises(EnvironmentError, match="GCS_BUCKET_NAME"):
        reload_config({"GCS_BUCKET_NAME": ""})


def test_config_default_values():
    cfg = reload_config({})
    assert cfg.GCS_BRONZE_PREFIX == "bronze"
    assert cfg.SPOTIFY_MARKET == "FR"
    assert cfg.PAGE_SIZE == 10
    assert cfg.BIGQUERY_DATASET_BRONZE == "spotify_bronze"
    assert cfg.BIGQUERY_DATASET_SILVER == "spotify_silver"
    assert cfg.BIGQUERY_DATASET_GOLD == "spotify_gold"


def test_config_max_pages_is_int():
    cfg = reload_config({"MAX_PAGES": "50"})
    assert cfg.MAX_PAGES == 50
    assert isinstance(cfg.MAX_PAGES, int)


def test_config_custom_values():
    cfg = reload_config({
        "SPOTIFY_MARKET": "US",
        "MAX_PAGES": "5",
        "GCS_BRONZE_PREFIX": "raw",
    })
    assert cfg.SPOTIFY_MARKET == "US"
    assert cfg.MAX_PAGES == 5
    assert cfg.GCS_BRONZE_PREFIX == "raw"
