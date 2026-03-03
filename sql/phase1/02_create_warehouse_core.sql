BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.fact_listings (
  listing_id TEXT PRIMARY KEY,
  detail_url TEXT,
  title TEXT,
  province TEXT,
  district TEXT,
  timeline_hours INTEGER,
  area_m2 NUMERIC,
  bedrooms INTEGER,
  bathrooms INTEGER,
  floors INTEGER,
  frontage NUMERIC,
  price_million_vnd NUMERIC,
  price_per_m2 NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_fact_listings_province_district
  ON warehouse.fact_listings (province, district);

CREATE INDEX IF NOT EXISTS idx_fact_listings_timeline_hours
  ON warehouse.fact_listings (timeline_hours);

CREATE INDEX IF NOT EXISTS idx_fact_listings_price_per_m2
  ON warehouse.fact_listings (price_per_m2);

CREATE TABLE IF NOT EXISTS warehouse.dim_location (
  location_id SERIAL PRIMARY KEY,
  province TEXT,
  district TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_location_province_district
  ON warehouse.dim_location (province, district);

CREATE TABLE IF NOT EXISTS warehouse.etl_run_log (
  run_id BIGSERIAL PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMP WITHOUT TIME ZONE,
  status TEXT NOT NULL,
  raw_source_rows BIGINT,
  raw_landing_rows BIGINT,
  staging_rows BIGINT,
  fact_rows BIGINT,
  dim_location_rows BIGINT,
  mart_market_analytics_rows BIGINT,
  mart_avm_features_rows BIGINT,
  quality_metrics JSONB,
  config JSONB,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_etl_run_log_pipeline_started_at
  ON warehouse.etl_run_log (pipeline_name, started_at DESC);

COMMIT;
