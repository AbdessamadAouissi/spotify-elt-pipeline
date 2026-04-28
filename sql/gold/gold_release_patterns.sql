-- ============================================================
-- GOLD : patterns de sortie — saisonnalité, jour, fréquence
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_GOLD_DATASET.release_patterns`
AS
WITH base AS (
  SELECT
    id,
    name,
    release_date,
    album_type,
    primary_artist_id,
    primary_artist_name,
    total_tracks,
    EXTRACT(YEAR        FROM release_date)  AS release_year,
    EXTRACT(MONTH       FROM release_date)  AS release_month,
    EXTRACT(DAYOFWEEK   FROM release_date)  AS release_dow,      -- 1=Dim, 6=Ven
    EXTRACT(QUARTER     FROM release_date)  AS release_quarter,
    FORMAT_DATE('%A',  release_date)        AS release_day_name,
    FORMAT_DATE('%B',  release_date)        AS release_month_name
  FROM `YOUR_PROJECT.YOUR_SILVER_DATASET.albums`
  WHERE release_date IS NOT NULL
    AND release_date_precision = 'day'     -- exclure les dates approximatives
),
artist_cadence AS (
  SELECT
    primary_artist_id,
    COUNT(*)                                            AS total_releases,
    MIN(release_date)                                   AS first_release,
    MAX(release_date)                                   AS last_release,
    DATE_DIFF(MAX(release_date), MIN(release_date), DAY) AS active_days,
    ROUND(
      DATE_DIFF(MAX(release_date), MIN(release_date), DAY)
      / NULLIF(COUNT(*) - 1, 0), 0
    )                                                   AS avg_days_between_releases
  FROM base
  GROUP BY primary_artist_id
)
SELECT
  b.release_year,
  b.release_quarter,
  b.release_month,
  b.release_month_name,
  b.release_dow,
  b.release_day_name,
  b.album_type,
  b.primary_artist_id,
  b.primary_artist_name,

  COUNT(*)                                            AS releases_count,

  -- Saisonnalité : % des sorties ce mois sur l'année
  ROUND(
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
      PARTITION BY b.release_year
    ), 2
  )                                                   AS pct_of_year,

  -- Vendredi : jour de sortie standard dans l'industrie musicale
  COUNTIF(b.release_dow = 6)                          AS friday_releases,
  ROUND(COUNTIF(b.release_dow = 6) * 100.0 / COUNT(*), 1) AS pct_friday,

  ac.avg_days_between_releases,
  ac.total_releases                                   AS artist_total_releases

FROM base b
LEFT JOIN artist_cadence ac ON b.primary_artist_id = ac.primary_artist_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, ac.avg_days_between_releases, ac.total_releases
ORDER BY release_year DESC, release_month DESC;
