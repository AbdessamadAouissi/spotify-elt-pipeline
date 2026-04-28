-- ============================================================
-- GOLD : tendances albums — volume par période, type, artiste
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_GOLD_DATASET.album_trends`
AS
-- Les window functions ne peuvent pas référencer release_date brut dans PARTITION BY
-- car il n'est pas dans le GROUP BY → on agrège d'abord dans une sous-requête
SELECT
  release_year,
  release_month,
  year_month,
  album_type,
  primary_artist_id,
  primary_artist_name,
  album_count,
  avg_tracks_per_album,
  max_tracks,
  min_tracks,

  -- Progression cumulée par année
  SUM(album_count) OVER (
    PARTITION BY release_year
    ORDER BY release_month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                                 AS cumulative_albums_in_year,

  -- Part par type d'album dans l'année
  ROUND(
    album_count * 100.0 / SUM(album_count) OVER (
      PARTITION BY release_year
    ), 2
  )                                                 AS pct_of_year_by_type

FROM (
  SELECT
    EXTRACT(YEAR  FROM release_date)                AS release_year,
    EXTRACT(MONTH FROM release_date)                AS release_month,
    FORMAT_DATE('%Y-%m', release_date)              AS year_month,
    album_type,
    primary_artist_id,
    primary_artist_name,
    COUNT(*)                                        AS album_count,
    ROUND(AVG(total_tracks), 1)                     AS avg_tracks_per_album,
    MAX(total_tracks)                               AS max_tracks,
    MIN(total_tracks)                               AS min_tracks
  FROM `YOUR_PROJECT.YOUR_SILVER_DATASET.albums`
  WHERE release_date IS NOT NULL
    AND EXTRACT(YEAR FROM release_date) BETWEEN 2000 AND EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY 1, 2, 3, 4, 5, 6
)
ORDER BY release_year DESC, release_month DESC;
