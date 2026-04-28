{{
  config(materialized = 'table')
}}

WITH bucketed AS (
  SELECT
    id,
    name,
    owner_id,
    owner_name,
    is_public,
    is_collaborative,
    CAST(track_count AS INT64)                AS track_count,
    extraction_date,

    CASE
      WHEN CAST(track_count AS INT64) < 10    THEN 'micro (< 10)'
      WHEN CAST(track_count AS INT64) < 50    THEN 'small (10-49)'
      WHEN CAST(track_count AS INT64) < 200   THEN 'medium (50-199)'
      WHEN CAST(track_count AS INT64) < 1000  THEN 'large (200-999)'
      ELSE 'mega (1000+)'
    END                                       AS size_bucket

  FROM {{ ref('stg_playlists') }}
  WHERE track_count IS NOT NULL
)

SELECT
  size_bucket,
  is_public,
  is_collaborative,

  COUNT(*)                                    AS playlist_count,
  ROUND(AVG(track_count), 1)                  AS avg_tracks,
  MAX(track_count)                            AS max_tracks,
  MIN(track_count)                            AS min_tracks,
  APPROX_QUANTILES(track_count, 2)[OFFSET(1)] AS median_tracks,

  COUNT(DISTINCT owner_id)                    AS unique_owners,

  ARRAY_AGG(
    owner_name ORDER BY track_count DESC LIMIT 1
  )[OFFSET(0)]                                AS top_owner_by_tracks,

  ROUND(
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
  )                                           AS pct_of_total

FROM bucketed
GROUP BY size_bucket, is_public, is_collaborative
ORDER BY avg_tracks DESC
