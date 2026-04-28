{{
  config(materialized = 'table')
}}

WITH artist_base AS (
  SELECT
    id                          AS artist_id,
    name                        AS artist_name,
    spotify_url,
    image_url
  FROM {{ ref('stg_artists') }}
),

track_counts AS (
  SELECT
    primary_artist_id                       AS artist_id,
    COUNT(*)                                AS total_tracks,
    ROUND(AVG(duration_min), 2)             AS avg_track_duration_min,
    COUNTIF(explicit)                       AS explicit_track_count
  FROM {{ ref('stg_tracks') }}
  GROUP BY primary_artist_id
),

album_counts AS (
  SELECT
    primary_artist_id             AS artist_id,
    COUNT(*)                      AS total_albums,
    MIN(release_date)             AS first_album_date,
    MAX(release_date)             AS latest_album_date
  FROM {{ ref('stg_albums') }}
  GROUP BY primary_artist_id
)

SELECT
  a.artist_id,
  a.artist_name,
  a.spotify_url,
  a.image_url,
  COALESCE(t.total_tracks, 0)           AS total_tracks,
  t.avg_track_duration_min,
  COALESCE(t.explicit_track_count, 0)   AS explicit_track_count,
  COALESCE(al.total_albums, 0)          AS total_albums,
  al.first_album_date,
  al.latest_album_date,
  DATE_DIFF(al.latest_album_date, al.first_album_date, YEAR) AS career_span_years
FROM artist_base a
LEFT JOIN track_counts  t  USING (artist_id)
LEFT JOIN album_counts  al USING (artist_id)
ORDER BY t.total_tracks DESC NULLS LAST
