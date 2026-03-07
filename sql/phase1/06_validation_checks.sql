-- Run after ETL to validate quality and warehouse integrity.

-- 1) Row-count sanity.
SELECT 'staging_count' AS check_name, COUNT(*)::TEXT AS check_value
FROM staging.stg_listings_raw
UNION ALL
SELECT 'staging_raw_text_count', COUNT(*)::TEXT
FROM staging.stg_listings_raw_text
UNION ALL
SELECT 'fact_count', COUNT(*)::TEXT
FROM warehouse.fact_listings
UNION ALL
SELECT 'dim_location_count', COUNT(*)::TEXT
FROM warehouse.dim_location
UNION ALL
SELECT 'mart_market_analytics_count', COUNT(*)::TEXT
FROM warehouse.mart_market_analytics
UNION ALL
SELECT 'mart_avm_features_count', COUNT(*)::TEXT
FROM warehouse.mart_avm_features;

-- 2) Invalid values should be zero in fact.
SELECT
  SUM(CASE WHEN price_million_vnd <= 0 OR price_million_vnd IS NULL THEN 1 ELSE 0 END) AS invalid_price_rows,
  SUM(CASE WHEN area_m2 <= 0 OR area_m2 IS NULL THEN 1 ELSE 0 END) AS invalid_area_rows,
  SUM(CASE WHEN price_per_m2 <= 0 OR price_per_m2 IS NULL THEN 1 ELSE 0 END) AS invalid_price_per_m2_rows
FROM warehouse.fact_listings;

-- 3) Duplicate checks in fact.
SELECT
  COUNT(*) - COUNT(DISTINCT listing_id) AS duplicate_listing_id_rows,
  COUNT(*) - COUNT(DISTINCT COALESCE(detail_url, CONCAT('id:', listing_id))) AS duplicate_dedupe_key_rows
FROM warehouse.fact_listings;

-- 4) Location normalization completeness.
SELECT
  SUM(CASE WHEN province IS NULL OR province = '' THEN 1 ELSE 0 END) AS null_or_empty_province,
  SUM(CASE WHEN district IS NULL OR district = '' THEN 1 ELSE 0 END) AS null_or_empty_district
FROM warehouse.fact_listings;

-- 5) Deterministic metric check for price_per_m2.
SELECT
  COUNT(*) AS mismatched_rows
FROM warehouse.fact_listings
WHERE ABS(price_per_m2 - (price_million_vnd / NULLIF(area_m2, 0))) > 0.0001;

-- 6) Referential completeness for AVM mart.
SELECT
  COUNT(*) AS avm_rows_without_location_id
FROM warehouse.mart_avm_features
WHERE location_id IS NULL;

-- 7) Outlier flag completeness in AVM mart.
SELECT
  SUM(CASE WHEN bedrooms_missing IS NULL THEN 1 ELSE 0 END) AS null_bedrooms_missing,
  SUM(CASE WHEN bathrooms_missing IS NULL THEN 1 ELSE 0 END) AS null_bathrooms_missing,
  SUM(CASE WHEN floors_missing IS NULL THEN 1 ELSE 0 END) AS null_floors_missing,
  SUM(CASE WHEN bedrooms_imputed IS NULL THEN 1 ELSE 0 END) AS null_bedrooms_imputed,
  SUM(CASE WHEN bathrooms_imputed IS NULL THEN 1 ELSE 0 END) AS null_bathrooms_imputed,
  SUM(CASE WHEN floors_imputed IS NULL THEN 1 ELSE 0 END) AS null_floors_imputed,
  SUM(CASE WHEN is_outlier_price IS NULL THEN 1 ELSE 0 END) AS null_is_outlier_price,
  SUM(CASE WHEN is_outlier_area IS NULL THEN 1 ELSE 0 END) AS null_is_outlier_area,
  SUM(CASE WHEN is_outlier_price_per_m2 IS NULL THEN 1 ELSE 0 END) AS null_is_outlier_price_per_m2,
  SUM(CASE WHEN is_outlier_any IS NULL THEN 1 ELSE 0 END) AS null_is_outlier_any,
  SUM(CASE WHEN is_robust_train_candidate IS NULL THEN 1 ELSE 0 END) AS null_is_robust_train_candidate,
  SUM(CASE WHEN is_outlier_any THEN 1 ELSE 0 END) AS flagged_outlier_rows,
  SUM(CASE WHEN is_robust_train_candidate THEN 1 ELSE 0 END) AS robust_train_candidate_rows
FROM warehouse.mart_avm_features;

-- 8) Raw-missing rows should be fully covered by imputed columns.
SELECT
  SUM(CASE WHEN bedrooms IS NULL AND bedrooms_imputed IS NULL THEN 1 ELSE 0 END) AS uncovered_bedrooms_missing,
  SUM(CASE WHEN bathrooms IS NULL AND bathrooms_imputed IS NULL THEN 1 ELSE 0 END) AS uncovered_bathrooms_missing,
  SUM(CASE WHEN floors IS NULL AND floors_imputed IS NULL THEN 1 ELSE 0 END) AS uncovered_floors_missing
FROM warehouse.mart_avm_features;

-- 9) Latest ETL run status.
SELECT
  run_id,
  pipeline_name,
  status,
  started_at,
  finished_at,
  raw_source_rows,
  raw_landing_rows,
  staging_rows,
  fact_rows,
  mart_market_analytics_rows,
  mart_avm_features_rows,
  error_message
FROM warehouse.etl_run_log
ORDER BY run_id DESC
LIMIT 5;
