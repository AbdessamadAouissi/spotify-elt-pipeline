# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spotify ELT pipeline using a medallion architecture: Spotify API → GCS (Bronze) → BigQuery Silver → BigQuery Gold → Looker Studio.

**GCP Project:** `smart-charter-301917` — All BigQuery datasets are in region `EU`.

---

## Environment Setup

### Two virtual environments

| Venv | Python | Purpose |
|---|---|---|
| `.venv/` | System Python | Extractors, loaders, tests (`requirements.txt`) |
| `.venv_dbt/` | Python 3.12 | dbt only (Python 3.14 breaks dbt/mashumaro) |

```bash
# Activate for extraction/loading
.venv\Scripts\activate

# Activate for dbt
.venv_dbt\Scripts\activate
```

### Required `.env` (copy from `.env.example`)
```
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
GCP_PROJECT_ID=smart-charter-301917
GCS_BUCKET_NAME=...
```

### GCP authentication
```bash
gcloud auth application-default login
```

---

## Common Commands

### Run the full Spotify API extraction
```bash
python pipeline.py                          # all resources
python pipeline.py --resources artists albums tracks
```
Resources: `albums`, `artists`, `tracks`, `artist_albums`, `album_tracks`, `playlists`

### Load Spotify Charts CSVs into BigQuery
```bash
python loaders/charts_loader.py --charts-dir charts/
```
CSV filename format: `regional-{country}-weekly-YYYY-MM-DD.csv`  
Download from: https://charts.spotify.com

### Deploy Looker Studio BigQuery views
```bash
GCP_PROJECT_ID=smart-charter-301917 python looker/create_looker_views.py
```

### dbt (must use `.venv_dbt`)
```bash
cd dbt
set GCP_PROJECT_ID=smart-charter-301917   # Windows
# or: export GCP_PROJECT_ID=smart-charter-301917  (bash)

dbt run                                   # build all models
dbt run --select silver.*                 # build silver only
dbt run --select gold.track_streams       # single model
dbt test                                  # run all 25 tests
dbt test --select stg_tracks              # single model tests
```

### Run tests
```bash
pytest                          # all tests
pytest tests/test_auth.py       # single file
pytest --cov=. --cov-report=term-missing
```

---

## Architecture

### Data Flow
```
Spotify API
    └─> extractors/*_extractor.py   (BaseExtractor subclasses, paginated, retried)
    └─> transformers/ndjson_transformer.py  (dict → NDJSON bytes)
    └─> loaders/gcs_loader.py       (upload to GCS, Hive-partitioned path)
    └─> GCS: bronze/{resource}/year=.../month=.../day=.../

BigQuery External Tables (spotify_bronze)
    └─> hive-partitioned NDJSON, explicit schemas (release_date as STRING)

dbt (dbt/ directory)
    Silver models (spotify_silver):
        stg_artists, stg_albums, stg_tracks, stg_playlists
        — dedup via ROW_NUMBER() OVER (PARTITION BY id ORDER BY extraction_timestamp DESC)
        — SAFE.PARSE_DATE + REGEXP_CONTAINS for partial Spotify dates ("1997", "1997-03")
    Gold models (spotify_gold):
        top_artists          — artist metrics aggregated from Silver
        album_trends         — monthly release trends with window functions
        release_patterns     — day-of-week / seasonality analysis
        playlist_insights    — playlist segmentation (APPROX_QUANTILES)
        track_streams        — joins spotify_charts + stg_tracks + stg_artists

Looker Studio (spotify_looker dataset)
    6 views created by looker/create_looker_views.py
    Dashboard guide: looker/DASHBOARD_GUIDE.md
```

### Key design decisions

- **Bronze tables use explicit schemas** (not autodetect) to prevent BigQuery from inferring `release_date` as DATE when Spotify returns year-only strings like `"1997"`.
- **`dbt/macros/generate_schema_name.sql`** overrides dbt's default behavior so `+schema: spotify_gold` creates dataset `spotify_gold` instead of `spotify_silver_spotify_gold`.
- **`track_streams`** (Gold) reads `spotify_charts` directly from `spotify_silver` (loaded by `charts_loader.py`) — it does not come from the Spotify API extraction.
- **Spotify Client Credentials** apps get 403 on `/artists?ids=...` since 2023 — `popularity`, `followers`, `genres` are not available and are absent from all models.
- **`stg_tracks`** UNIONs two Bronze sources: `tracks` (from /search) and `album_tracks` (from /albums/{id}/tracks), deduped by `id`.

### Extractor pattern

All extractors inherit `BaseExtractor` and implement `extract() -> Generator[dict]`. The base class handles:
- Exponential retry (tenacity, 5 attempts) on HTTP errors and 429 rate limits
- `_enrich()`: adds `_extraction_timestamp`, `_extraction_date`, `_market` to every record
- `paginate()`: iterates Spotify cursor-based pagination up to `MAX_PAGES`

`artist_albums` and `album_tracks` extractors require Silver to be populated first (they iterate IDs from `stg_artists` / `stg_albums`).

### BigQuery datasets

| Dataset | Contents |
|---|---|
| `spotify_bronze` | External tables → GCS NDJSON |
| `spotify_silver` | dbt Silver models + `spotify_charts` (from charts_loader) |
| `spotify_gold` | dbt Gold models |
| `spotify_looker` | Optimised views for Looker Studio |

---

## Weekly Update Workflow

1. Download new CSVs from https://charts.spotify.com → place in `charts/`
2. `python loaders/charts_loader.py --charts-dir charts/`
3. `cd dbt && dbt run --select gold.track_streams`
4. Refresh data in Looker Studio
