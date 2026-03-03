BEGIN;

TRUNCATE TABLE warehouse.mart_market_analytics;

WITH fact_buckets AS (
  SELECT
    province,
    district,
    CASE
      WHEN timeline_hours IS NULL THEN 'unknown'
      WHEN timeline_hours < 24 THEN '0_24h'
      WHEN timeline_hours < 72 THEN '24_72h'
      WHEN timeline_hours < 168 THEN '3_7d'
      WHEN timeline_hours < 720 THEN '8_30d'
      ELSE 'gt_30d'
    END AS time_bucket,
    price_million_vnd,
    price_per_m2
  FROM warehouse.fact_listings
)
INSERT INTO warehouse.mart_market_analytics (
  province,
  district,
  time_bucket,
  median_price_million_vnd,
  median_price_per_m2,
  listing_count,
  refreshed_at
)
SELECT
  province,
  district,
  time_bucket,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_price_million_vnd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2,
  COUNT(*) AS listing_count,
  NOW() AS refreshed_at
FROM fact_buckets
GROUP BY province, district, time_bucket;

TRUNCATE TABLE warehouse.mart_avm_features;

WITH district_stats AS (
  SELECT
    province,
    district,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS district_median_price_million_vnd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS district_median_price_per_m2
  FROM warehouse.fact_listings
  GROUP BY province, district
),
fact_buckets AS (
  SELECT
    f.*,
    CASE
      WHEN f.timeline_hours IS NULL THEN 'unknown'
      WHEN f.timeline_hours < 24 THEN '0_24h'
      WHEN f.timeline_hours < 72 THEN '24_72h'
      WHEN f.timeline_hours < 168 THEN '3_7d'
      WHEN f.timeline_hours < 720 THEN '8_30d'
      ELSE 'gt_30d'
    END AS time_bucket
  FROM warehouse.fact_listings f
)
INSERT INTO warehouse.mart_avm_features (
  listing_id,
  location_id,
  province,
  district,
  timeline_hours,
  time_bucket,
  area_m2,
  bedrooms,
  bathrooms,
  floors,
  frontage,
  district_median_price_million_vnd,
  district_median_price_per_m2,
  target_price_million_vnd,
  target_price_per_m2,
  refreshed_at
)
SELECT
  fb.listing_id,
  dl.location_id,
  fb.province,
  fb.district,
  fb.timeline_hours,
  fb.time_bucket,
  fb.area_m2,
  fb.bedrooms,
  fb.bathrooms,
  fb.floors,
  fb.frontage,
  ds.district_median_price_million_vnd,
  ds.district_median_price_per_m2,
  fb.price_million_vnd AS target_price_million_vnd,
  fb.price_per_m2 AS target_price_per_m2,
  NOW() AS refreshed_at
FROM fact_buckets fb
LEFT JOIN warehouse.dim_location dl
  ON dl.province = fb.province
 AND dl.district = fb.district
LEFT JOIN district_stats ds
  ON ds.province = fb.province
 AND ds.district = fb.district;

COMMIT;
