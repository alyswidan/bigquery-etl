WITH base AS (
  SELECT
    *
  FROM
    activity_stream_stable.impression_stats_v1
  WHERE
    (@submission_date IS NULL OR @submission_date = DATE(submission_timestamp))
),
impression_data AS (
  SELECT
    *
  FROM
    base
  WHERE
    -- make sure data is valid/non-empty
    ARRAY_LENGTH(tiles) >= 1
)
SELECT
  -- truncate timestamp to seconds
  TIMESTAMP_TRUNC(submission_timestamp, SECOND) AS submission_timestamp,
  impression_id AS client_id, -- client_id renamed to impression_id in GCP
  flattened_tiles.id AS tile_id,
  addon_version,
  SUM(CASE WHEN loaded IS NOT NULL THEN 1 ELSE 0 END) AS loaded,
  SUM(
    CASE
    WHEN click IS NULL
    AND block IS NULL
    AND pocket IS NULL
    AND loaded IS NULL THEN 1
    ELSE 0
    END
  ) AS impressions,
  SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS clicks,
  SUM(CASE WHEN block IS NOT NULL THEN 1 ELSE 0 END) AS blocked,
  SUM(CASE WHEN pocket IS NOT NULL THEN 1 ELSE 0 END) AS pocketed,
  -- the 3x1 layout has a bug where we need to use the position of each element
  -- in the tiles array instead of the actual pos field
  IFNULL(flattened_tiles.pos, alt_pos) AS position,
  page,
  source,
  locale,
  normalized_country_code AS country_code,
  version,
  user_prefs,
  release_channel,
  CASE
  WHEN shield_id IS NULL THEN 'n/a'
  ELSE shield_id
  END AS shield_id,
  sample_id
FROM
  impression_data
CROSS JOIN
  UNNEST(impression_data.tiles) AS flattened_tiles
WITH OFFSET AS alt_pos
GROUP BY
  submission_timestamp,
  impression_id,
  tile_id,
  addon_version,
  position,
  page,
  source,
  locale,
  country_code,
  version,
  user_prefs,
  release_channel,
  shield_id,
  sample_id
