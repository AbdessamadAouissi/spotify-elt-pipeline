{{
  config(materialized = 'table')
}}

/*
  Vue Gold : streams Spotify Charts enrichis avec métadonnées artiste + titre.

  Sources :
    - spotify_silver.spotify_charts  (chargé via loaders/charts_loader.py)
    - stg_tracks                     (durée, explicit, album)
    - stg_artists                    (nom artiste)

  Permet dans Looker Studio de filtrer par :
    - Période (chart_date)
    - Pays (country)
    - Artiste
    - Titre
*/

WITH charts AS (
  SELECT
    chart_date,
    country,
    chart_type,
    rank,
    track_id,
    track_name,
    artist_names,
    streams,
    peak_rank,
    weeks_on_chart,
    EXTRACT(YEAR  FROM CAST(chart_date AS DATE)) AS chart_year,
    EXTRACT(MONTH FROM CAST(chart_date AS DATE)) AS chart_month,
    FORMAT_DATE('%Y-%m', CAST(chart_date AS DATE)) AS year_month
  FROM `{{ env_var('GCP_PROJECT_ID') }}.{{ var('silver_dataset', 'spotify_silver') }}.spotify_charts`
),

tracks AS (
  SELECT
    id              AS track_id,
    name            AS track_name_clean,
    duration_min,
    explicit,
    album_release_date,
    primary_artist_id
  FROM {{ ref('stg_tracks') }}
),

artists AS (
  SELECT
    id          AS artist_id,
    name        AS artist_name
  FROM {{ ref('stg_artists') }}
)

SELECT
  -- Dimensions charts
  c.chart_date,
  c.country,
  c.chart_type,
  c.chart_year,
  c.chart_month,
  c.year_month,
  c.rank,
  c.peak_rank,
  c.weeks_on_chart,

  -- Track
  c.track_id,
  COALESCE(t.track_name_clean, c.track_name)    AS track_name,
  c.artist_names,
  t.duration_min,
  t.explicit,
  t.album_release_date,

  -- Artiste
  a.artist_name,

  -- Métriques streams
  c.streams,
  SUM(c.streams) OVER (
    PARTITION BY c.track_id, c.country
    ORDER BY c.chart_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                             AS cumulative_streams,

  ROUND(
    c.streams * 100.0 / NULLIF(SUM(c.streams) OVER (
      PARTITION BY c.chart_year, c.chart_month, c.country
    ), 0), 4
  )                                             AS pct_monthly_streams

FROM charts c
LEFT JOIN tracks  t ON c.track_id = t.track_id
LEFT JOIN artists a ON t.primary_artist_id = a.artist_id
ORDER BY c.chart_date DESC, c.rank ASC
