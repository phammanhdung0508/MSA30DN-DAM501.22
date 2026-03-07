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
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS district_median_price_per_m2,
    COUNT(bedrooms) AS district_bedrooms_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bedrooms) FILTER (WHERE bedrooms IS NOT NULL) AS district_median_bedrooms,
    COUNT(bathrooms) AS district_bathrooms_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bathrooms) FILTER (WHERE bathrooms IS NOT NULL) AS district_median_bathrooms,
    COUNT(floors) AS district_floors_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY floors) FILTER (WHERE floors IS NOT NULL) AS district_median_floors
  FROM warehouse.fact_listings
  GROUP BY province, district
),
province_stats AS (
  SELECT
    province,
    COUNT(bedrooms) AS province_bedrooms_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bedrooms) FILTER (WHERE bedrooms IS NOT NULL) AS province_median_bedrooms,
    COUNT(bathrooms) AS province_bathrooms_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bathrooms) FILTER (WHERE bathrooms IS NOT NULL) AS province_median_bathrooms,
    COUNT(floors) AS province_floors_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY floors) FILTER (WHERE floors IS NOT NULL) AS province_median_floors
  FROM warehouse.fact_listings
  GROUP BY province
),
global_stats AS (
  SELECT
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bedrooms) FILTER (WHERE bedrooms IS NOT NULL) AS global_median_bedrooms,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bathrooms) FILTER (WHERE bathrooms IS NOT NULL) AS global_median_bathrooms,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY floors) FILTER (WHERE floors IS NOT NULL) AS global_median_floors
  FROM warehouse.fact_listings
),
outlier_bounds AS (
  SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_million_vnd) AS price_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_million_vnd) AS price_q3,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY area_m2) AS area_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY area_m2) AS area_q3,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_m2) AS ppm2_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_m2) AS ppm2_q3
  FROM warehouse.fact_listings
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
),
avm_base AS (
  SELECT
    fb.listing_id,
    fb.province,
    fb.district,
    fb.timeline_hours,
    fb.time_bucket,
    fb.area_m2,
    fb.bedrooms,
    (fb.bedrooms IS NULL) AS bedrooms_missing,
    COALESCE(
      fb.bedrooms,
      CASE WHEN ds.district_bedrooms_count >= 30 THEN ds.district_median_bedrooms END,
      CASE WHEN ps.province_bedrooms_count >= 30 THEN ps.province_median_bedrooms END,
      gs.global_median_bedrooms
    )::INTEGER AS bedrooms_imputed,
    fb.bathrooms,
    (fb.bathrooms IS NULL) AS bathrooms_missing,
    COALESCE(
      fb.bathrooms,
      CASE WHEN ds.district_bathrooms_count >= 30 THEN ds.district_median_bathrooms END,
      CASE WHEN ps.province_bathrooms_count >= 30 THEN ps.province_median_bathrooms END,
      gs.global_median_bathrooms
    )::INTEGER AS bathrooms_imputed,
    fb.floors,
    (fb.floors IS NULL) AS floors_missing,
    COALESCE(
      fb.floors,
      CASE WHEN ds.district_floors_count >= 30 THEN ds.district_median_floors END,
      CASE WHEN ps.province_floors_count >= 30 THEN ps.province_median_floors END,
      gs.global_median_floors
    )::INTEGER AS floors_imputed,
    fb.frontage,
    ds.district_median_price_million_vnd,
    ds.district_median_price_per_m2,
    fb.price_million_vnd,
    fb.price_per_m2,
    (
      fb.price_million_vnd < (ob.price_q1 - 1.5 * (ob.price_q3 - ob.price_q1))
      OR fb.price_million_vnd > (ob.price_q3 + 1.5 * (ob.price_q3 - ob.price_q1))
    ) AS is_outlier_price,
    (
      fb.area_m2 < (ob.area_q1 - 1.5 * (ob.area_q3 - ob.area_q1))
      OR fb.area_m2 > (ob.area_q3 + 1.5 * (ob.area_q3 - ob.area_q1))
    ) AS is_outlier_area,
    (
      fb.price_per_m2 < (ob.ppm2_q1 - 1.5 * (ob.ppm2_q3 - ob.ppm2_q1))
      OR fb.price_per_m2 > (ob.ppm2_q3 + 1.5 * (ob.ppm2_q3 - ob.ppm2_q1))
    ) AS is_outlier_price_per_m2
  FROM fact_buckets fb
  CROSS JOIN outlier_bounds ob
  CROSS JOIN global_stats gs
  LEFT JOIN district_stats ds
    ON ds.province = fb.province
   AND ds.district = fb.district
  LEFT JOIN province_stats ps
    ON ps.province = fb.province
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
  bedrooms_missing,
  bedrooms_imputed,
  bathrooms,
  bathrooms_missing,
  bathrooms_imputed,
  floors,
  floors_missing,
  floors_imputed,
  frontage,
  district_median_price_million_vnd,
  district_median_price_per_m2,
  is_outlier_price,
  is_outlier_area,
  is_outlier_price_per_m2,
  is_outlier_any,
  is_robust_train_candidate,
  target_price_million_vnd,
  target_price_per_m2,
  refreshed_at
)
SELECT
  ab.listing_id,
  dl.location_id,
  ab.province,
  ab.district,
  ab.timeline_hours,
  ab.time_bucket,
  ab.area_m2,
  ab.bedrooms,
  ab.bedrooms_missing,
  ab.bedrooms_imputed,
  ab.bathrooms,
  ab.bathrooms_missing,
  ab.bathrooms_imputed,
  ab.floors,
  ab.floors_missing,
  ab.floors_imputed,
  ab.frontage,
  ab.district_median_price_million_vnd,
  ab.district_median_price_per_m2,
  ab.is_outlier_price,
  ab.is_outlier_area,
  ab.is_outlier_price_per_m2,
  (ab.is_outlier_price OR ab.is_outlier_area OR ab.is_outlier_price_per_m2) AS is_outlier_any,
  NOT (ab.is_outlier_price OR ab.is_outlier_area OR ab.is_outlier_price_per_m2) AS is_robust_train_candidate,
  ab.price_million_vnd AS target_price_million_vnd,
  ab.price_per_m2 AS target_price_per_m2,
  NOW() AS refreshed_at
FROM avm_base ab
LEFT JOIN warehouse.dim_location dl
  ON dl.province = ab.province
 AND dl.district = ab.district;

COMMIT;
