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

WITH all_albums AS (
  -- Source principale : albums issus de la recherche Spotify (/search)
  -- Note : artist_albums sera ajouté quand la table Bronze sera disponible
  SELECT
    id, name, album_type, total_tracks,
    release_date, release_date_precision,
    artists, images, external_urls,
    _extraction_date, _extraction_timestamp
  FROM {{ source('bronze', 'albums') }}
  WHERE id IS NOT NULL AND name IS NOT NULL
),

ranked AS (
  SELECT
    id,
    TRIM(name)                                        AS name,
    album_type,
    total_tracks,
    -- Normalise les dates partielles Spotify ("1997" → "1997-01-01", "1997-03" → "1997-03-01")
    SAFE.PARSE_DATE('%Y-%m-%d',
      CASE
        WHEN REGEXP_CONTAINS(release_date, r'^\d{4}$')       THEN CONCAT(release_date, '-01-01')
        WHEN REGEXP_CONTAINS(release_date, r'^\d{4}-\d{2}$') THEN CONCAT(release_date, '-01')
        ELSE release_date
      END
    )                                                 AS release_date,
    release_date_precision,
    artists[SAFE_OFFSET(0)].id                        AS primary_artist_id,
    artists[SAFE_OFFSET(0)].name                      AS primary_artist_name,
    ARRAY_LENGTH(artists)                             AS artist_count,
    external_urls.spotify                             AS spotify_url,
    images[SAFE_OFFSET(0)].url                        AS image_url,
    _extraction_date                                  AS extraction_date,
    _extraction_timestamp                             AS extraction_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY _extraction_timestamp DESC
    )                                                 AS rn
  FROM all_albums
)

SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1
