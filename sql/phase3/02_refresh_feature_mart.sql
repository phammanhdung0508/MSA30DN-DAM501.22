BEGIN;

TRUNCATE TABLE warehouse.mart_avm_features_final;

WITH base AS (
  SELECT
    listing_id,
    location_id,
    province,
    district,
    timeline_hours,
    area_m2,
    bedrooms_imputed,
    bedrooms_missing,
    bathrooms_imputed,
    bathrooms_missing,
    floors_imputed,
    floors_missing,
    frontage,
    district_median_price_million_vnd,
    district_median_price_per_m2,
    is_outlier_price,
    is_outlier_area,
    is_outlier_price_per_m2,
    is_outlier_any,
    is_robust_train_candidate,
    target_price_million_vnd,
    target_price_per_m2
  FROM warehouse.mart_avm_features
),
total_rows AS (
  SELECT COUNT(*)::NUMERIC AS row_count
  FROM base
),
province_stats AS (
  SELECT
    province,
    COUNT(*)::INTEGER AS province_listing_count_ref,
    COUNT(*)::NUMERIC / NULLIF((SELECT row_count FROM total_rows), 0) AS province_frequency_ref,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY target_price_million_vnd) AS province_median_price_million_vnd_ref,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY target_price_per_m2) AS province_median_price_per_m2_ref
  FROM base
  GROUP BY province
),
district_stats AS (
  SELECT
    province,
    district,
    COUNT(*)::INTEGER AS district_listing_count_ref,
    COUNT(*)::NUMERIC / NULLIF((SELECT row_count FROM total_rows), 0) AS district_frequency_ref,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY target_price_million_vnd) AS district_median_price_million_vnd_ref,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY target_price_per_m2) AS district_median_price_per_m2_ref
  FROM base
  GROUP BY province, district
),
quantiles AS (
  SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY area_m2) AS area_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY area_m2) AS area_p75,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY area_m2) AS area_p99,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY bedrooms_imputed) AS bedrooms_p99,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY bathrooms_imputed) AS bathrooms_p99,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY floors_imputed) AS floors_p99,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY timeline_hours) FILTER (WHERE timeline_hours IS NOT NULL) AS timeline_p50,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY timeline_hours) FILTER (WHERE timeline_hours IS NOT NULL) AS timeline_p99
  FROM base
),
assembled AS (
  SELECT
    b.listing_id,
    b.location_id,
    b.province,
    b.district,
    ps.province_listing_count_ref,
    ds.district_listing_count_ref,
    ps.province_frequency_ref,
    ds.district_frequency_ref,
    ps.province_median_price_million_vnd_ref,
    ps.province_median_price_per_m2_ref,
    ds.district_median_price_million_vnd_ref,
    ds.district_median_price_per_m2_ref,
    ds.district_median_price_million_vnd_ref / NULLIF(ps.province_median_price_million_vnd_ref, 0) AS district_to_province_price_ratio_ref,
    ds.district_median_price_per_m2_ref / NULLIF(ps.province_median_price_per_m2_ref, 0) AS district_to_province_price_per_m2_ratio_ref,
    b.timeline_hours,
    (b.timeline_hours IS NULL) AS timeline_missing,
    COALESCE(b.timeline_hours, q.timeline_p50::INTEGER) AS timeline_hours_imputed,
    LEAST(COALESCE(b.timeline_hours::NUMERIC, q.timeline_p50), q.timeline_p99) AS timeline_hours_capped_p99,
    COALESCE(b.timeline_hours::NUMERIC, q.timeline_p50) / 24.0 AS timeline_days_imputed,
    LN(1 + COALESCE(b.timeline_hours::NUMERIC, q.timeline_p50)) AS timeline_log_hours,
    CASE
      WHEN b.timeline_hours IS NULL THEN 'unknown'
      WHEN b.timeline_hours < 24 THEN '0_24h'
      WHEN b.timeline_hours < 72 THEN '1_3d'
      WHEN b.timeline_hours < 168 THEN '3_7d'
      WHEN b.timeline_hours < 720 THEN '7_30d'
      ELSE 'gt_30d'
    END AS timeline_bucket,
    COALESCE(b.timeline_hours, q.timeline_p50::INTEGER) < 72 AS is_new_listing,
    COALESCE(b.timeline_hours, q.timeline_p50::INTEGER) > 720 AS is_stale_listing,
    b.area_m2,
    LEAST(b.area_m2, q.area_p99) AS area_m2_capped_p99,
    LN(1 + b.area_m2) AS log_area_m2,
    b.bedrooms_imputed,
    b.bedrooms_missing,
    LEAST(b.bedrooms_imputed::NUMERIC, q.bedrooms_p99) AS bedrooms_capped_p99,
    b.bathrooms_imputed,
    b.bathrooms_missing,
    LEAST(b.bathrooms_imputed::NUMERIC, q.bathrooms_p99) AS bathrooms_capped_p99,
    b.floors_imputed,
    b.floors_missing,
    LEAST(b.floors_imputed::NUMERIC, q.floors_p99) AS floors_capped_p99,
    b.frontage AS frontage_raw,
    (b.frontage IS NULL) AS frontage_missing,
    COALESCE(b.frontage, 0) > 0 AS has_frontage,
    (b.bedrooms_imputed + b.bathrooms_imputed) AS total_rooms,
    b.area_m2 / NULLIF(GREATEST(b.bedrooms_imputed + b.bathrooms_imputed, 1), 0) AS area_per_room,
    b.bedrooms_imputed::NUMERIC / NULLIF(b.area_m2, 0) AS bedroom_density,
    b.bathrooms_imputed::NUMERIC / NULLIF(b.area_m2, 0) AS bathroom_density,
    (CASE WHEN COALESCE(b.frontage, 0) > 0 THEN 1 ELSE 0 END) * LN(1 + b.area_m2) AS has_frontage_x_log_area,
    b.area_m2 >= q.area_p75 AS is_large_property,
    b.floors_imputed >= 2 AS is_multi_floor,
    b.is_outlier_area,
    b.is_outlier_price,
    b.is_outlier_price_per_m2,
    b.is_outlier_any,
    b.is_robust_train_candidate,
    b.target_price_million_vnd,
    LN(1 + b.target_price_million_vnd) AS target_log_price_million_vnd,
    b.target_price_per_m2,
    LN(1 + b.target_price_per_m2) AS target_log_price_per_m2,
    'phase3_v1'::TEXT AS feature_version,
    NOW() AS refreshed_at
  FROM base b
  CROSS JOIN quantiles q
  LEFT JOIN province_stats ps
    ON ps.province = b.province
  LEFT JOIN district_stats ds
    ON ds.province = b.province
   AND ds.district = b.district
)
INSERT INTO warehouse.mart_avm_features_final (
  listing_id,
  location_id,
  province,
  district,
  province_listing_count_ref,
  district_listing_count_ref,
  province_frequency_ref,
  district_frequency_ref,
  province_median_price_million_vnd_ref,
  province_median_price_per_m2_ref,
  district_median_price_million_vnd_ref,
  district_median_price_per_m2_ref,
  district_to_province_price_ratio_ref,
  district_to_province_price_per_m2_ratio_ref,
  timeline_hours,
  timeline_missing,
  timeline_hours_imputed,
  timeline_hours_capped_p99,
  timeline_days_imputed,
  timeline_log_hours,
  timeline_bucket,
  is_new_listing,
  is_stale_listing,
  area_m2,
  area_m2_capped_p99,
  log_area_m2,
  bedrooms_imputed,
  bedrooms_missing,
  bedrooms_capped_p99,
  bathrooms_imputed,
  bathrooms_missing,
  bathrooms_capped_p99,
  floors_imputed,
  floors_missing,
  floors_capped_p99,
  frontage_raw,
  frontage_missing,
  has_frontage,
  total_rooms,
  area_per_room,
  bedroom_density,
  bathroom_density,
  has_frontage_x_log_area,
  is_large_property,
  is_multi_floor,
  is_outlier_area,
  is_outlier_price,
  is_outlier_price_per_m2,
  is_outlier_any,
  is_robust_train_candidate,
  target_price_million_vnd,
  target_log_price_million_vnd,
  target_price_per_m2,
  target_log_price_per_m2,
  feature_version,
  refreshed_at
)
SELECT
  listing_id,
  location_id,
  province,
  district,
  province_listing_count_ref,
  district_listing_count_ref,
  province_frequency_ref,
  district_frequency_ref,
  province_median_price_million_vnd_ref,
  province_median_price_per_m2_ref,
  district_median_price_million_vnd_ref,
  district_median_price_per_m2_ref,
  district_to_province_price_ratio_ref,
  district_to_province_price_per_m2_ratio_ref,
  timeline_hours,
  timeline_missing,
  timeline_hours_imputed,
  timeline_hours_capped_p99,
  timeline_days_imputed,
  timeline_log_hours,
  timeline_bucket,
  is_new_listing,
  is_stale_listing,
  area_m2,
  area_m2_capped_p99,
  log_area_m2,
  bedrooms_imputed,
  bedrooms_missing,
  bedrooms_capped_p99,
  bathrooms_imputed,
  bathrooms_missing,
  bathrooms_capped_p99,
  floors_imputed,
  floors_missing,
  floors_capped_p99,
  frontage_raw,
  frontage_missing,
  has_frontage,
  total_rooms,
  area_per_room,
  bedroom_density,
  bathroom_density,
  has_frontage_x_log_area,
  is_large_property,
  is_multi_floor,
  is_outlier_area,
  is_outlier_price,
  is_outlier_price_per_m2,
  is_outlier_any,
  is_robust_train_candidate,
  target_price_million_vnd,
  target_log_price_million_vnd,
  target_price_per_m2,
  target_log_price_per_m2,
  feature_version,
  refreshed_at
FROM assembled;

COMMIT;
