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

WITH ranked AS (
  SELECT
    id,
    TRIM(name)                                        AS name,
    TRIM(description)                                 AS description,
    public                                            AS is_public,
    collaborative                                     AS is_collaborative,
    items.total                                       AS track_count,

    -- Propriétaire
    owner.id                                          AS owner_id,
    owner.display_name                                AS owner_name,

    -- Image principale
    images[SAFE_OFFSET(0)].url                        AS image_url,
    external_urls.spotify                             AS spotify_url,

    _extraction_date                                  AS extraction_date,
    _extraction_timestamp                             AS extraction_timestamp,

    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY _extraction_timestamp DESC
    )                                                 AS rn

  FROM {{ source('bronze', 'playlists') }}
  WHERE id IS NOT NULL
    AND name IS NOT NULL
)

SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1
