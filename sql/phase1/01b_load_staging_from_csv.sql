-- Usage:
-- psql "postgresql://<user>:<password>@<host>:<port>/<db>" \
--   -v csv_path='data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv' \
--   -f sql/phase1/01b_load_staging_from_csv.sql
--
-- Note:
-- The source CSV stores frontage as boolean (true/false).
-- This loader only performs type harmonization to match staging.frontage NUMERIC.

BEGIN;

TRUNCATE TABLE staging.stg_listings_raw_text;

\copy staging.stg_listings_raw_text (
  id,
  detail_url,
  title,
  location,
  timeline_hours,
  area_m2,
  bedrooms,
  bathrooms,
  floors,
  frontage,
  price_million_vnd
) FROM :'csv_path' WITH (
  FORMAT csv,
  HEADER true,
  ENCODING 'UTF8'
);

TRUNCATE TABLE staging.stg_listings_raw;

INSERT INTO staging.stg_listings_raw (
  id,
  detail_url,
  title,
  location,
  timeline_hours,
  area_m2,
  bedrooms,
  bathrooms,
  floors,
  frontage,
  price_million_vnd
)
SELECT
  NULLIF(TRIM(id), ''),
  NULLIF(TRIM(detail_url), ''),
  NULLIF(TRIM(title), ''),
  NULLIF(TRIM(location), ''),
  NULLIF(TRIM(timeline_hours), '')::INTEGER,
  NULLIF(TRIM(area_m2), '')::NUMERIC,
  NULLIF(TRIM(bedrooms), '')::INTEGER,
  NULLIF(TRIM(bathrooms), '')::INTEGER,
  NULLIF(TRIM(floors), '')::INTEGER,
  CASE
    WHEN LOWER(TRIM(frontage)) IN ('true', 't', '1') THEN 1
    WHEN LOWER(TRIM(frontage)) IN ('false', 'f', '0') THEN 0
    WHEN TRIM(frontage) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM(frontage)::NUMERIC
    ELSE NULL
  END,
  NULLIF(TRIM(price_million_vnd), '')::NUMERIC
FROM staging.stg_listings_raw_text;

COMMIT;
