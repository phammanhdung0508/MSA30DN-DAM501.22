BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_listings_raw_text (
  id TEXT,
  detail_url TEXT,
  title TEXT,
  location TEXT,
  timeline_hours TEXT,
  area_m2 TEXT,
  bedrooms TEXT,
  bathrooms TEXT,
  floors TEXT,
  frontage TEXT,
  price_million_vnd TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_listings_raw_text_id
  ON staging.stg_listings_raw_text (id);

CREATE INDEX IF NOT EXISTS idx_stg_listings_raw_text_detail_url
  ON staging.stg_listings_raw_text (detail_url);

CREATE TABLE IF NOT EXISTS staging.stg_listings_raw (
  id TEXT,
  detail_url TEXT,
  title TEXT,
  location TEXT,
  timeline_hours INTEGER,
  area_m2 NUMERIC,
  bedrooms INTEGER,
  bathrooms INTEGER,
  floors INTEGER,
  frontage NUMERIC,
  price_million_vnd NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_stg_listings_raw_id
  ON staging.stg_listings_raw (id);

CREATE INDEX IF NOT EXISTS idx_stg_listings_raw_detail_url
  ON staging.stg_listings_raw (detail_url);

COMMIT;
