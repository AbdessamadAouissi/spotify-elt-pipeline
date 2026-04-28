-- ============================================================
-- SILVER : tracks — fusion search + album_tracks, déduplication
-- Sources : bronze.tracks (recherche) + bronze.album_tracks (albums)
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_SILVER_DATASET.tracks`
PARTITION BY extraction_date
CLUSTER BY id
AS
WITH all_tracks AS (
  -- Source 1 : tracks issus de la recherche Spotify
  SELECT
    id,
    TRIM(name)                                          AS name,
    duration_ms,
    ROUND(CAST(duration_ms AS FLOAT64) / 60000, 2)     AS duration_min,
    explicit,
    track_number,
    disc_number,
    album.id                                            AS album_id,
    album.name                                          AS album_name,
    album.release_date                                  AS album_release_date,
    album.album_type                                    AS album_type,
    artists[SAFE_OFFSET(0)].id                          AS primary_artist_id,
    artists[SAFE_OFFSET(0)].name                        AS primary_artist_name,
    external_urls.spotify                               AS spotify_url,
    _extraction_date                                    AS extraction_date,
    _extraction_timestamp                               AS extraction_timestamp
  FROM `YOUR_PROJECT.YOUR_BRONZE_DATASET.tracks`
  WHERE id IS NOT NULL AND name IS NOT NULL

  UNION ALL

  -- Source 2 : tracks extraits album par album (données complètes)
  SELECT
    id,
    TRIM(name)                                          AS name,
    duration_ms,
    ROUND(CAST(duration_ms AS FLOAT64) / 60000, 2)     AS duration_min,
    explicit,
    track_number,
    disc_number,
    album.id                                            AS album_id,
    album.name                                          AS album_name,
    album.release_date                                  AS album_release_date,
    album.album_type                                    AS album_type,
    artists[SAFE_OFFSET(0)].id                          AS primary_artist_id,
    artists[SAFE_OFFSET(0)].name                        AS primary_artist_name,
    external_urls.spotify                               AS spotify_url,
    _extraction_date                                    AS extraction_date,
    _extraction_timestamp                               AS extraction_timestamp
  FROM `YOUR_PROJECT.YOUR_BRONZE_DATASET.album_tracks`
  WHERE id IS NOT NULL AND name IS NOT NULL
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY extraction_timestamp DESC
    ) AS rn
  FROM all_tracks
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
