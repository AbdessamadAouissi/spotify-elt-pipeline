{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'extraction_date',
      'data_type': 'date'
    },
    cluster_by = ['id']
  )
}}

WITH all_tracks AS (
  -- Source 1 : titres issus de la recherche Spotify
  SELECT
    id,
    TRIM(name)                                        AS name,
    duration_ms,
    ROUND(CAST(duration_ms AS FLOAT64) / 60000, 2)   AS duration_min,
    explicit,
    track_number,
    disc_number,
    album.id                                          AS album_id,
    album.name                                        AS album_name,
    -- Normalise les dates partielles Spotify
    SAFE.PARSE_DATE('%Y-%m-%d',
      CASE
        WHEN REGEXP_CONTAINS(album.release_date, r'^\d{4}$')       THEN CONCAT(album.release_date, '-01-01')
        WHEN REGEXP_CONTAINS(album.release_date, r'^\d{4}-\d{2}$') THEN CONCAT(album.release_date, '-01')
        ELSE album.release_date
      END
    )                                                 AS album_release_date,
    album.album_type                                  AS album_type,
    artists[SAFE_OFFSET(0)].id                        AS primary_artist_id,
    artists[SAFE_OFFSET(0)].name                      AS primary_artist_name,
    external_urls.spotify                             AS spotify_url,
    _extraction_date                                  AS extraction_date,
    _extraction_timestamp                             AS extraction_timestamp
  FROM {{ source('bronze', 'tracks') }}
  WHERE id IS NOT NULL AND name IS NOT NULL

  UNION ALL

  -- Source 2 : titres extraits album par album (données complètes)
  SELECT
    id,
    TRIM(name)                                        AS name,
    duration_ms,
    ROUND(CAST(duration_ms AS FLOAT64) / 60000, 2)   AS duration_min,
    explicit,
    track_number,
    disc_number,
    album.id                                          AS album_id,
    album.name                                        AS album_name,
    SAFE.PARSE_DATE('%Y-%m-%d',
      CASE
        WHEN REGEXP_CONTAINS(album.release_date, r'^\d{4}$')       THEN CONCAT(album.release_date, '-01-01')
        WHEN REGEXP_CONTAINS(album.release_date, r'^\d{4}-\d{2}$') THEN CONCAT(album.release_date, '-01')
        ELSE album.release_date
      END
    )                                                 AS album_release_date,
    album.album_type                                  AS album_type,
    artists[SAFE_OFFSET(0)].id                        AS primary_artist_id,
    artists[SAFE_OFFSET(0)].name                      AS primary_artist_name,
    external_urls.spotify                             AS spotify_url,
    _extraction_date                                  AS extraction_date,
    _extraction_timestamp                             AS extraction_timestamp
  FROM {{ source('bronze', 'album_tracks') }}
  WHERE id IS NOT NULL AND name IS NOT NULL
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY extraction_timestamp DESC
    ) AS rn
  FROM all_tracks
)

SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1
