-- ============================================================
-- BRONZE : Tables externes BigQuery pointant sur GCS (NDJSON)
-- Remplacer YOUR_PROJECT, YOUR_BUCKET, YOUR_BRONZE_DATASET
-- ============================================================

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.YOUR_BRONZE_DATASET`
OPTIONS (location = 'EU');

-- ── Artists ──────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT.YOUR_BRONZE_DATASET.artists`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64,
  day   INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://YOUR_BUCKET/bronze/artists/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/bronze/artists'
);

-- ── Albums ───────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT.YOUR_BRONZE_DATASET.albums`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64,
  day   INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://YOUR_BUCKET/bronze/albums/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/bronze/albums'
);

-- ── Tracks ───────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT.YOUR_BRONZE_DATASET.tracks`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64,
  day   INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://YOUR_BUCKET/bronze/tracks/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/bronze/tracks'
);

-- ── Album Tracks (tracks extraits album par album) ───────────
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT.YOUR_BRONZE_DATASET.album_tracks`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64,
  day   INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://YOUR_BUCKET/bronze/album_tracks/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/bronze/album_tracks'
);

-- ── Playlists ────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT.YOUR_BRONZE_DATASET.playlists`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64,
  day   INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://YOUR_BUCKET/bronze/playlists/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/bronze/playlists'
);
