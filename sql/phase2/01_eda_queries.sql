-- Phase 2 EDA queries for DAM501.22 (warehouse.fact_listings)
-- Run example:
--   export PGPASSWORD='postgres'
--   psql -h localhost -p 5432 -U postgres -d 'DAM501.22' -f sql/phase2/01_eda_queries.sql

\echo '=== 1) DATA QUALITY VALIDATION ==='

-- 1.1 Row count
SELECT COUNT(*) AS fact_row_count
FROM warehouse.fact_listings;

-- 1.2 Null / missing summary for key columns
WITH base AS (
  SELECT *
  FROM warehouse.fact_listings
), total AS (
  SELECT COUNT(*)::NUMERIC AS total_rows FROM base
), metrics AS (
  SELECT 'price_million_vnd' AS column_name, COUNT(*) FILTER (WHERE price_million_vnd IS NULL)::NUMERIC AS missing_count FROM base
  UNION ALL
  SELECT 'area_m2', COUNT(*) FILTER (WHERE area_m2 IS NULL)::NUMERIC FROM base
  UNION ALL
  SELECT 'bedrooms', COUNT(*) FILTER (WHERE bedrooms IS NULL)::NUMERIC FROM base
  UNION ALL
  SELECT 'bathrooms', COUNT(*) FILTER (WHERE bathrooms IS NULL)::NUMERIC FROM base
  UNION ALL
  SELECT 'floors', COUNT(*) FILTER (WHERE floors IS NULL)::NUMERIC FROM base
  UNION ALL
  SELECT 'frontage', COUNT(*) FILTER (WHERE frontage IS NULL)::NUMERIC FROM base
  UNION ALL
  SELECT 'province', COUNT(*) FILTER (WHERE province IS NULL OR BTRIM(province) = '')::NUMERIC FROM base
  UNION ALL
  SELECT 'district', COUNT(*) FILTER (WHERE district IS NULL OR BTRIM(district) = '')::NUMERIC FROM base
)
SELECT
  m.column_name,
  m.missing_count::BIGINT AS missing_count,
  ROUND(100.0 * m.missing_count / NULLIF(t.total_rows, 0), 4) AS missing_pct
FROM metrics m
CROSS JOIN total t
ORDER BY m.column_name;

-- 1.3 Numeric ranges
SELECT
  'price_million_vnd' AS metric,
  MIN(price_million_vnd) AS min_value,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_value,
  MAX(price_million_vnd) AS max_value
FROM warehouse.fact_listings
UNION ALL
SELECT
  'area_m2',
  MIN(area_m2),
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY area_m2),
  MAX(area_m2)
FROM warehouse.fact_listings
UNION ALL
SELECT
  'bedrooms',
  MIN(bedrooms::NUMERIC),
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bedrooms),
  MAX(bedrooms::NUMERIC)
FROM warehouse.fact_listings
UNION ALL
SELECT
  'bathrooms',
  MIN(bathrooms::NUMERIC),
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bathrooms),
  MAX(bathrooms::NUMERIC)
FROM warehouse.fact_listings
UNION ALL
SELECT
  'floors',
  MIN(floors::NUMERIC),
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY floors),
  MAX(floors::NUMERIC)
FROM warehouse.fact_listings
UNION ALL
SELECT
  'frontage',
  MIN(frontage),
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY frontage),
  MAX(frontage)
FROM warehouse.fact_listings
ORDER BY metric;

-- 1.4 Duplicate checks
SELECT
  COUNT(*) - COUNT(DISTINCT listing_id) AS duplicate_listing_id_rows,
  COUNT(*) - COUNT(DISTINCT COALESCE(NULLIF(BTRIM(detail_url), ''), 'id:' || listing_id)) AS duplicate_dedupe_key_rows
FROM warehouse.fact_listings;

\echo '=== 2) TARGET VARIABLE ANALYSIS ==='

-- 2.1 Distribution summary + skewness proxies
WITH base AS (
  SELECT price_million_vnd
  FROM warehouse.fact_listings
  WHERE price_million_vnd IS NOT NULL
),
stats AS (
  SELECT
    COUNT(*)::BIGINT AS n,
    AVG(price_million_vnd) AS mean_price,
    STDDEV_POP(price_million_vnd) AS std_price
  FROM base
)
SELECT
  s.n,
  s.mean_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.price_million_vnd) AS median_price,
  s.std_price,
  CASE
    WHEN s.std_price IS NULL OR s.std_price = 0 THEN NULL
    ELSE AVG(POWER(b.price_million_vnd - s.mean_price, 3)) / POWER(s.std_price, 3)
  END AS skewness_price
FROM base b
CROSS JOIN stats s
GROUP BY s.n, s.mean_price, s.std_price;

SELECT
  COUNT(*) AS n,
  AVG(LN(price_million_vnd + 1)) AS mean_log_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LN(price_million_vnd + 1)) AS median_log_price,
  STDDEV_POP(LN(price_million_vnd + 1)) AS std_log_price
FROM warehouse.fact_listings;

SELECT
  COUNT(*) AS n,
  AVG(price_per_m2) AS mean_price_per_m2,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2,
  STDDEV_POP(price_per_m2) AS std_price_per_m2
FROM warehouse.fact_listings;

\echo '=== 3) LOCATION-BASED MARKET ANALYSIS ==='

-- 3.1 Listing count + median price by province
SELECT
  province,
  COUNT(*) AS listing_count,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_price_million_vnd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2
FROM warehouse.fact_listings
GROUP BY province
ORDER BY median_price_per_m2 DESC, listing_count DESC;

-- 3.2 District median price_per_m2 (filter tiny groups)
SELECT
  province,
  district,
  COUNT(*) AS listing_count,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2
FROM warehouse.fact_listings
GROUP BY province, district
HAVING COUNT(*) >= 30
ORDER BY median_price_per_m2 DESC, listing_count DESC;

\echo '=== 4) PROPERTY SIZE ANALYSIS ==='

-- 4.1 Pearson correlations with area
SELECT
  CORR(area_m2, price_million_vnd) AS corr_area_price,
  CORR(area_m2, price_per_m2) AS corr_area_price_per_m2
FROM warehouse.fact_listings;

-- 4.2 Extreme outlier candidates by area and price_per_m2
SELECT
  listing_id,
  province,
  district,
  area_m2,
  price_million_vnd,
  price_per_m2
FROM warehouse.fact_listings
ORDER BY area_m2 DESC, price_per_m2 DESC
LIMIT 50;

\echo '=== 5) PROPERTY FEATURE ANALYSIS ==='

-- 5.1 Bedrooms effect on price
SELECT
  bedrooms,
  COUNT(*) AS listing_count,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2
FROM warehouse.fact_listings
WHERE bedrooms IS NOT NULL
GROUP BY bedrooms
ORDER BY bedrooms;

-- 5.2 Floors effect on price
SELECT
  floors,
  COUNT(*) AS listing_count,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2
FROM warehouse.fact_listings
WHERE floors IS NOT NULL
GROUP BY floors
ORDER BY floors;

\echo '=== 6) CORRELATION ANALYSIS ==='

SELECT
  CORR(area_m2, price_million_vnd) AS corr_area_price,
  CORR(area_m2, price_per_m2) AS corr_area_ppm2,
  CORR(bedrooms::NUMERIC, price_million_vnd) AS corr_bedrooms_price,
  CORR(bathrooms::NUMERIC, price_million_vnd) AS corr_bathrooms_price,
  CORR(floors::NUMERIC, price_million_vnd) AS corr_floors_price,
  CORR(frontage, price_million_vnd) AS corr_frontage_price,
  CORR(bedrooms::NUMERIC, bathrooms::NUMERIC) AS corr_bedrooms_bathrooms,
  CORR(bedrooms::NUMERIC, floors::NUMERIC) AS corr_bedrooms_floors,
  CORR(price_million_vnd, price_per_m2) AS corr_price_ppm2
FROM warehouse.fact_listings;

\echo '=== 7) TIMELINE ANALYSIS ==='

WITH bucketed AS (
  SELECT
    CASE
      WHEN timeline_hours IS NULL THEN 'unknown'
      WHEN timeline_hours < 24 THEN '0_24h'
      WHEN timeline_hours < 72 THEN '1_3d'
      WHEN timeline_hours < 168 THEN '3_7d'
      WHEN timeline_hours < 720 THEN '7_30d'
      ELSE 'gt_30d'
    END AS time_bucket,
    price_million_vnd,
    price_per_m2
  FROM warehouse.fact_listings
)
SELECT
  time_bucket,
  COUNT(*) AS listing_count,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_price_million_vnd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2
FROM bucketed
GROUP BY time_bucket
ORDER BY CASE time_bucket
  WHEN '0_24h' THEN 1
  WHEN '1_3d' THEN 2
  WHEN '3_7d' THEN 3
  WHEN '7_30d' THEN 4
  WHEN 'gt_30d' THEN 5
  ELSE 99
END;

\echo '=== 8) MART VALIDATION ==='

WITH fact_rebuilt AS (
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
    COUNT(*) AS listing_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_million_vnd) AS median_price_million_vnd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_price_per_m2
  FROM warehouse.fact_listings
  GROUP BY province, district,
    CASE
      WHEN timeline_hours IS NULL THEN 'unknown'
      WHEN timeline_hours < 24 THEN '0_24h'
      WHEN timeline_hours < 72 THEN '24_72h'
      WHEN timeline_hours < 168 THEN '3_7d'
      WHEN timeline_hours < 720 THEN '8_30d'
      ELSE 'gt_30d'
    END
)
SELECT
  COUNT(*) FILTER (
    WHERE m.province IS NULL
       OR f.province IS NULL
       OR COALESCE(m.listing_count, -1) <> COALESCE(f.listing_count, -1)
       OR ABS(COALESCE(m.median_price_million_vnd, -1) - COALESCE(f.median_price_million_vnd, -1)) > 1e-9
       OR ABS(COALESCE(m.median_price_per_m2, -1) - COALESCE(f.median_price_per_m2, -1)) > 1e-9
  ) AS mismatch_group_count,
  COUNT(*) AS compared_group_count
FROM warehouse.mart_market_analytics m
FULL OUTER JOIN fact_rebuilt f
  ON m.province = f.province
 AND m.district = f.district
 AND m.time_bucket = f.time_bucket;

\echo '=== 9) AVM FEATURE CANDIDATES QUICK STATS ==='

SELECT
  COUNT(*) AS row_count,
  AVG((area_m2 IS NULL)::INT)::NUMERIC(6,4) AS miss_area_m2,
  AVG((bedrooms IS NULL)::INT)::NUMERIC(6,4) AS miss_bedrooms,
  AVG((bathrooms IS NULL)::INT)::NUMERIC(6,4) AS miss_bathrooms,
  AVG((floors IS NULL)::INT)::NUMERIC(6,4) AS miss_floors,
  AVG((frontage IS NULL)::INT)::NUMERIC(6,4) AS miss_frontage,
  AVG((timeline_hours IS NULL)::INT)::NUMERIC(6,4) AS miss_timeline_hours,
  COUNT(DISTINCT province) AS n_province,
  COUNT(DISTINCT district) AS n_district
FROM warehouse.fact_listings;

SELECT
  SUM(CASE WHEN bedrooms_missing THEN 1 ELSE 0 END) AS bedrooms_missing_rows,
  SUM(CASE WHEN bathrooms_missing THEN 1 ELSE 0 END) AS bathrooms_missing_rows,
  SUM(CASE WHEN floors_missing THEN 1 ELSE 0 END) AS floors_missing_rows,
  SUM(CASE WHEN bedrooms_imputed IS NULL THEN 1 ELSE 0 END) AS null_bedrooms_imputed,
  SUM(CASE WHEN bathrooms_imputed IS NULL THEN 1 ELSE 0 END) AS null_bathrooms_imputed,
  SUM(CASE WHEN floors_imputed IS NULL THEN 1 ELSE 0 END) AS null_floors_imputed,
  SUM(CASE WHEN is_outlier_price THEN 1 ELSE 0 END) AS outlier_price_rows,
  SUM(CASE WHEN is_outlier_area THEN 1 ELSE 0 END) AS outlier_area_rows,
  SUM(CASE WHEN is_outlier_price_per_m2 THEN 1 ELSE 0 END) AS outlier_price_per_m2_rows,
  SUM(CASE WHEN is_outlier_any THEN 1 ELSE 0 END) AS outlier_any_rows,
  SUM(CASE WHEN is_robust_train_candidate THEN 1 ELSE 0 END) AS robust_train_candidate_rows
FROM warehouse.mart_avm_features;
