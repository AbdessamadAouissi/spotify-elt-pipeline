-- ============================================================
-- SILVER : albums — fusion /search + /artists/{id}/albums, déduplication
-- Sources : bronze.albums (recherche) + bronze.artist_albums (discographie)
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_SILVER_DATASET.albums`
PARTITION BY extraction_date
CLUSTER BY id
AS
WITH all_albums AS (
  -- Source 1 : albums issus de la recherche Spotify
  SELECT
    id, name, album_type, total_tracks,
    release_date, release_date_precision,
    artists, images, external_urls,
    _extraction_date, _extraction_timestamp
  FROM `YOUR_PROJECT.YOUR_BRONZE_DATASET.albums`
  WHERE id IS NOT NULL AND name IS NOT NULL

  UNION ALL

  -- Source 2 : discographie complète par artiste (/artists/{id}/albums)
  SELECT
    id, name, album_type, total_tracks,
    release_date, release_date_precision,
    artists, images, external_urls,
    _extraction_date, _extraction_timestamp
  FROM `YOUR_PROJECT.YOUR_BRONZE_DATASET.artist_albums`
  WHERE id IS NOT NULL AND name IS NOT NULL
),
ranked AS (
  SELECT
    id,
    TRIM(name)                                          AS name,
    album_type,
    total_tracks,
    release_date,
    release_date_precision,
    artists[SAFE_OFFSET(0)].id                          AS primary_artist_id,
    artists[SAFE_OFFSET(0)].name                        AS primary_artist_name,
    ARRAY_LENGTH(artists)                               AS artist_count,
    external_urls.spotify                               AS spotify_url,
    images[SAFE_OFFSET(0)].url                          AS image_url,
    _extraction_date                                    AS extraction_date,
    _extraction_timestamp                               AS extraction_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY _extraction_timestamp DESC
    )                                                   AS rn
  FROM all_albums
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
