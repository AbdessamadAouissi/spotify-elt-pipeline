-- ============================================================
-- BRONZE : table externe artist_albums
-- Créée APRÈS l'extraction (Phase 3) car BigQuery exige
-- qu'au moins un fichier existe avant de créer la table.
-- ============================================================

CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT.YOUR_BRONZE_DATASET.artist_albums`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64,
  day   INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://YOUR_BUCKET/bronze/artist_albums/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/bronze/artist_albums'
);
