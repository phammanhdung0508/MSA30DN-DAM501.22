-- Full-refresh transform from staging to warehouse core.
-- Primary business cleaning rules:
-- 1) drop invalid price/area (<=0)
-- 2) de-duplicate by detail_url priority (fallback to id if detail_url missing)
-- 3) parse district/province from free-text location
-- 4) compute price_per_m2

BEGIN;

TRUNCATE TABLE warehouse.fact_listings;

WITH src AS (
  SELECT
    NULLIF(TRIM(id), '') AS listing_id,
    NULLIF(TRIM(detail_url), '') AS detail_url,
    NULLIF(TRIM(title), '') AS title,
    NULLIF(TRIM(location), '') AS location,
    timeline_hours,
    area_m2,
    bedrooms,
    bathrooms,
    floors,
    frontage,
    price_million_vnd,
    COALESCE(NULLIF(TRIM(detail_url), ''), CONCAT('id:', NULLIF(TRIM(id), ''))) AS dedupe_key,
    (
      CASE WHEN bedrooms IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN bathrooms IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN floors IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN frontage IS NOT NULL THEN 1 ELSE 0 END
    ) AS completeness_score
  FROM staging.stg_listings_raw
),
valid AS (
  SELECT
    listing_id,
    detail_url,
    title,
    location,
    timeline_hours,
    area_m2,
    bedrooms,
    bathrooms,
    floors,
    frontage,
    price_million_vnd,
    NULLIF(TRIM(SPLIT_PART(location, ',', 1)), '') AS district_raw,
    NULLIF(TRIM(SPLIT_PART(location, ',', 2)), '') AS province_raw,
    dedupe_key,
    completeness_score
  FROM src
  WHERE listing_id IS NOT NULL
    AND COALESCE(price_million_vnd, 0) > 0
    AND COALESCE(area_m2, 0) > 0
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY dedupe_key
      ORDER BY
        timeline_hours ASC NULLS LAST,
        completeness_score DESC,
        listing_id ASC
    ) AS rn
  FROM valid
),
dedup AS (
  SELECT
    listing_id,
    detail_url,
    title,
    INITCAP(province_raw) AS province,
    INITCAP(district_raw) AS district,
    timeline_hours,
    area_m2,
    bedrooms,
    bathrooms,
    floors,
    frontage,
    price_million_vnd,
    price_million_vnd / NULLIF(area_m2, 0) AS price_per_m2
  FROM ranked
  WHERE rn = 1
)
INSERT INTO warehouse.fact_listings (
  listing_id,
  detail_url,
  title,
  province,
  district,
  timeline_hours,
  area_m2,
  bedrooms,
  bathrooms,
  floors,
  frontage,
  price_million_vnd,
  price_per_m2
)
SELECT
  listing_id,
  detail_url,
  title,
  province,
  district,
  timeline_hours,
  area_m2,
  bedrooms,
  bathrooms,
  floors,
  frontage,
  price_million_vnd,
  price_per_m2
FROM dedup;

TRUNCATE TABLE warehouse.dim_location RESTART IDENTITY;

INSERT INTO warehouse.dim_location (province, district)
SELECT DISTINCT
  province,
  district
FROM warehouse.fact_listings
WHERE province IS NOT NULL
   OR district IS NOT NULL
ORDER BY province, district;

COMMIT;
