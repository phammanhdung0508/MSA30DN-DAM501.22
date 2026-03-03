BEGIN;

CREATE TABLE IF NOT EXISTS warehouse.mart_market_analytics (
  province TEXT,
  district TEXT,
  time_bucket TEXT,
  median_price_million_vnd NUMERIC,
  median_price_per_m2 NUMERIC,
  listing_count BIGINT,
  refreshed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (province, district, time_bucket)
);

CREATE TABLE IF NOT EXISTS warehouse.mart_avm_features (
  listing_id TEXT PRIMARY KEY,
  location_id INTEGER,
  province TEXT,
  district TEXT,
  timeline_hours INTEGER,
  time_bucket TEXT,
  area_m2 NUMERIC,
  bedrooms INTEGER,
  bathrooms INTEGER,
  floors INTEGER,
  frontage NUMERIC,
  district_median_price_million_vnd NUMERIC,
  district_median_price_per_m2 NUMERIC,
  target_price_million_vnd NUMERIC,
  target_price_per_m2 NUMERIC,
  refreshed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mart_market_analytics_geo
  ON warehouse.mart_market_analytics (province, district);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_location_id
  ON warehouse.mart_avm_features (location_id);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_time_bucket
  ON warehouse.mart_avm_features (time_bucket);

COMMIT;
