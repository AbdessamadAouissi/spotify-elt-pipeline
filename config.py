import os
from dotenv import load_dotenv

load_dotenv()

_REQUIRED = [
    "SPOTIFY_CLIENT_ID",
    "SPOTIFY_CLIENT_SECRET",
    "GCP_PROJECT_ID",
    "GCS_BUCKET_NAME",
]
_missing = [k for k in _REQUIRED if not os.environ.get(k)]
if _missing:
    raise EnvironmentError(
        f"Variables d'environnement manquantes : {', '.join(_missing)}\n"
        "Copiez .env.example vers .env et renseignez les valeurs."
    )

SPOTIFY_CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
SPOTIFY_CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
GCS_BRONZE_PREFIX = os.getenv("GCS_BRONZE_PREFIX", "bronze")

BIGQUERY_DATASET_BRONZE = os.getenv("BIGQUERY_DATASET_BRONZE", "spotify_bronze")
BIGQUERY_DATASET_SILVER = os.getenv("BIGQUERY_DATASET_SILVER", "spotify_silver")
BIGQUERY_DATASET_GOLD = os.getenv("BIGQUERY_DATASET_GOLD", "spotify_gold")

SPOTIFY_SEARCH_QUERY          = os.getenv("SPOTIFY_SEARCH_QUERY",          "year:2020-2024")
SPOTIFY_SEARCH_QUERY_ARTISTS  = os.getenv("SPOTIFY_SEARCH_QUERY_ARTISTS",  "rap francais")
SPOTIFY_SEARCH_QUERY_PLAYLISTS = os.getenv("SPOTIFY_SEARCH_QUERY_PLAYLISTS", "rap francais")
SPOTIFY_MARKET = os.getenv("SPOTIFY_MARKET", "FR")
MAX_PAGES = int(os.getenv("MAX_PAGES", 10))
PAGE_SIZE = 10  # max autorisé par Spotify (restreint à 10 en 2025)
