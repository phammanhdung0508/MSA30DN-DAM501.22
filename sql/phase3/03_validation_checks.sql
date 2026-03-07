-- Phase 3 validation checks for warehouse.mart_avm_features_final.

-- 1) Row count should match source mart.
SELECT 'mart_avm_features_count' AS check_name, COUNT(*)::TEXT AS check_value
FROM warehouse.mart_avm_features
UNION ALL
SELECT 'mart_avm_features_final_count', COUNT(*)::TEXT
FROM warehouse.mart_avm_features_final;

-- 2) Primary key uniqueness.
SELECT
  COUNT(*) - COUNT(DISTINCT listing_id) AS duplicate_listing_id_rows
FROM warehouse.mart_avm_features_final;

-- 3) Required feature completeness.
SELECT
  SUM(CASE WHEN province IS NULL OR province = '' THEN 1 ELSE 0 END) AS null_province_rows,
  SUM(CASE WHEN district IS NULL OR district = '' THEN 1 ELSE 0 END) AS null_district_rows,
  SUM(CASE WHEN timeline_hours_imputed IS NULL THEN 1 ELSE 0 END) AS null_timeline_hours_imputed,
  SUM(CASE WHEN area_m2 IS NULL THEN 1 ELSE 0 END) AS null_area_rows,
  SUM(CASE WHEN log_area_m2 IS NULL THEN 1 ELSE 0 END) AS null_log_area_rows,
  SUM(CASE WHEN bedrooms_imputed IS NULL THEN 1 ELSE 0 END) AS null_bedrooms_imputed,
  SUM(CASE WHEN bathrooms_imputed IS NULL THEN 1 ELSE 0 END) AS null_bathrooms_imputed,
  SUM(CASE WHEN floors_imputed IS NULL THEN 1 ELSE 0 END) AS null_floors_imputed,
  SUM(CASE WHEN has_frontage IS NULL THEN 1 ELSE 0 END) AS null_has_frontage,
  SUM(CASE WHEN target_price_million_vnd IS NULL THEN 1 ELSE 0 END) AS null_target_price,
  SUM(CASE WHEN target_log_price_million_vnd IS NULL THEN 1 ELSE 0 END) AS null_target_log_price
FROM warehouse.mart_avm_features_final;

-- 4) Transform consistency checks.
SELECT
  COUNT(*) FILTER (WHERE ABS(log_area_m2 - LN(1 + area_m2)) > 0.000001) AS log_area_mismatch_rows,
  COUNT(*) FILTER (WHERE ABS(target_log_price_million_vnd - LN(1 + target_price_million_vnd)) > 0.000001) AS log_target_mismatch_rows,
  COUNT(*) FILTER (WHERE total_rooms <> (bedrooms_imputed + bathrooms_imputed)) AS total_rooms_mismatch_rows,
  COUNT(*) FILTER (WHERE has_frontage <> (COALESCE(frontage_raw, 0) > 0)) AS frontage_flag_mismatch_rows,
  COUNT(*) FILTER (WHERE area_per_room IS NULL OR area_per_room <= 0) AS invalid_area_per_room_rows
FROM warehouse.mart_avm_features_final;

-- 5) Reference feature completeness.
SELECT
  SUM(CASE WHEN province_listing_count_ref IS NULL THEN 1 ELSE 0 END) AS null_province_listing_count_ref,
  SUM(CASE WHEN district_listing_count_ref IS NULL THEN 1 ELSE 0 END) AS null_district_listing_count_ref,
  SUM(CASE WHEN province_median_price_million_vnd_ref IS NULL THEN 1 ELSE 0 END) AS null_province_median_price_ref,
  SUM(CASE WHEN district_median_price_million_vnd_ref IS NULL THEN 1 ELSE 0 END) AS null_district_median_price_ref,
  SUM(CASE WHEN district_to_province_price_ratio_ref IS NULL THEN 1 ELSE 0 END) AS null_district_to_province_price_ratio_ref
FROM warehouse.mart_avm_features_final;

-- 6) Optional filtering flags.
SELECT
  SUM(CASE WHEN is_outlier_area THEN 1 ELSE 0 END) AS area_outlier_rows,
  SUM(CASE WHEN is_outlier_price THEN 1 ELSE 0 END) AS price_outlier_rows,
  SUM(CASE WHEN is_outlier_price_per_m2 THEN 1 ELSE 0 END) AS price_per_m2_outlier_rows,
  SUM(CASE WHEN is_outlier_any THEN 1 ELSE 0 END) AS any_outlier_rows,
  SUM(CASE WHEN is_robust_train_candidate THEN 1 ELSE 0 END) AS robust_train_candidate_rows
FROM warehouse.mart_avm_features_final;
