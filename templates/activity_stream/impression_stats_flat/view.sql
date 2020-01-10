CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.activity_stream.impression_stats_flat`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.activity_stream_derived.impression_stats_flat_v1`
