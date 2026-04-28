-- ============================================================
-- SILVER : artists — nettoyage, typage, déduplication
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_SILVER_DATASET.artists`
PARTITION BY extraction_date
CLUSTER BY id
AS
WITH ranked AS (
  SELECT
    id,
    TRIM(name)                                          AS name,
    external_urls.spotify                               AS spotify_url,
    -- Image la plus grande (hauteur max)
    (SELECT i.url
     FROM UNNEST(images) AS i
     ORDER BY i.height DESC
     LIMIT 1)                                           AS image_url,
    _extraction_date                                    AS extraction_date,
    _extraction_timestamp                               AS extraction_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY _extraction_timestamp DESC
    )                                                   AS rn
  FROM `YOUR_PROJECT.YOUR_BRONZE_DATASET.artists`
  WHERE id IS NOT NULL
    AND name IS NOT NULL
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
