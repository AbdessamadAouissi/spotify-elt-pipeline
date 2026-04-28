-- ============================================================
-- Looker Studio — Vues optimisées (à créer dans BigQuery)
-- Projet : smart-charter-301917
-- Dataset source : spotify_gold
-- Dataset cible : spotify_looker  (nouveau dataset à créer)
-- ============================================================

-- ÉTAPE 0 : Créer le dataset spotify_looker dans BigQuery
-- Console GCP > BigQuery > Créer dataset > spotify_looker > Region: europe-west1

-- ============================================================
-- VUE 1 : lk_streams
-- Source Looker : "Spotify — Streams"
-- Utilisée dans : Page 1, 2, 3
-- ============================================================
CREATE OR REPLACE VIEW `smart-charter-301917.spotify_looker.lk_streams` AS
SELECT
  -- Dimensions temporelles
  chart_date,
  chart_year,
  chart_month,
  year_month,
  FORMAT_DATE('%b %Y', CAST(chart_date AS DATE))          AS month_label,

  -- Dimensions géo / type
  country,
  chart_type,

  -- Classement
  rank,
  peak_rank,
  weeks_on_chart,

  -- Track
  track_id,
  track_name,
  artist_names,
  artist_name,
  duration_min,
  explicit,
  album_release_date,

  -- Métriques streams
  streams,
  cumulative_streams,
  ROUND(pct_monthly_streams, 4)                           AS pct_monthly_streams,

  -- Champs calculés utiles dans Looker
  ROUND(streams / 1000000.0, 2)                           AS streams_m,
  CASE
    WHEN rank = 1    THEN '#1'
    WHEN rank <= 3   THEN 'Top 3'
    WHEN rank <= 10  THEN 'Top 10'
    WHEN rank <= 50  THEN 'Top 50'
    ELSE 'Top 200'
  END                                                     AS rank_bucket

FROM `smart-charter-301917.spotify_gold.track_streams`;


-- ============================================================
-- VUE 2 : lk_artists
-- Source Looker : "Spotify — Artistes"
-- Utilisée dans : Page 2
-- ============================================================
CREATE OR REPLACE VIEW `smart-charter-301917.spotify_looker.lk_artists` AS
SELECT
  artist_id,
  artist_name,
  spotify_url,
  image_url,
  total_tracks,
  avg_track_duration_min,
  explicit_track_count,
  ROUND(explicit_track_count * 100.0 / NULLIF(total_tracks, 0), 1)  AS pct_explicit,
  total_albums,
  first_album_date,
  latest_album_date,
  career_span_years
FROM `smart-charter-301917.spotify_gold.top_artists`;


-- ============================================================
-- VUE 3 : lk_album_trends
-- Source Looker : "Spotify — Albums"
-- Utilisée dans : Page 4
-- ============================================================
CREATE OR REPLACE VIEW `smart-charter-301917.spotify_looker.lk_album_trends` AS
SELECT
  release_year,
  release_month,
  year_month,
  album_type,
  primary_artist_name,
  album_count,
  avg_tracks_per_album,
  max_tracks,
  min_tracks,
  cumulative_albums_in_year,
  pct_of_year_by_type
FROM `smart-charter-301917.spotify_gold.album_trends`;


-- ============================================================
-- VUE 4 : lk_release_patterns
-- Source Looker : "Spotify — Release Patterns"
-- Utilisée dans : Page 4 (secondaire)
-- ============================================================
CREATE OR REPLACE VIEW `smart-charter-301917.spotify_looker.lk_release_patterns` AS
SELECT
  release_year,
  release_quarter,
  release_month,
  release_month_name,
  release_dow,
  release_day_name,
  album_type,
  primary_artist_name,
  releases_count,
  pct_of_year,
  friday_releases,
  pct_friday,
  avg_days_between_releases,
  artist_total_releases
FROM `smart-charter-301917.spotify_gold.release_patterns`;


-- ============================================================
-- VUE 5 : lk_playlists
-- Source Looker : "Spotify — Playlists"
-- Utilisée dans : Page 5
-- ============================================================
CREATE OR REPLACE VIEW `smart-charter-301917.spotify_looker.lk_playlist_insights` AS
SELECT
  size_bucket,
  is_public,
  is_collaborative,
  playlist_count,
  avg_tracks,
  max_tracks,
  min_tracks,
  median_tracks,
  unique_owners,
  top_owner_by_tracks,
  pct_of_total,

  -- Labels lisibles
  CASE WHEN is_public THEN 'Publique' ELSE 'Privée' END  AS visibilite,
  CASE WHEN is_collaborative THEN 'Collaborative' ELSE 'Solo' END AS type_edition

FROM `smart-charter-301917.spotify_gold.playlist_insights`;


-- ============================================================
-- VUE 6 (BONUS) : lk_streams_weekly_summary
-- Résumé hebdomadaire — idéal pour le scorecard "semaine en cours"
-- ============================================================
CREATE OR REPLACE VIEW `smart-charter-301917.spotify_looker.lk_streams_weekly_summary` AS
WITH latest_week AS (
  SELECT MAX(chart_date) AS max_date
  FROM `smart-charter-301917.spotify_gold.track_streams`
)
SELECT
  s.chart_date,
  s.country,
  s.rank,
  s.track_name,
  s.artist_names,
  s.streams,
  s.peak_rank,
  s.weeks_on_chart,
  s.rank_bucket
FROM `smart-charter-301917.spotify_looker.lk_streams` s
CROSS JOIN latest_week lw
WHERE s.chart_date = lw.max_date
ORDER BY s.country, s.rank;
