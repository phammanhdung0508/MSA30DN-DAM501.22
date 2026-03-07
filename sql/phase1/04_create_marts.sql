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
  bedrooms_missing BOOLEAN,
  bedrooms_imputed INTEGER,
  bathrooms INTEGER,
  bathrooms_missing BOOLEAN,
  bathrooms_imputed INTEGER,
  floors INTEGER,
  floors_missing BOOLEAN,
  floors_imputed INTEGER,
  frontage NUMERIC,
  district_median_price_million_vnd NUMERIC,
  district_median_price_per_m2 NUMERIC,
  is_outlier_price BOOLEAN,
  is_outlier_area BOOLEAN,
  is_outlier_price_per_m2 BOOLEAN,
  is_outlier_any BOOLEAN,
  is_robust_train_candidate BOOLEAN,
  target_price_million_vnd NUMERIC,
  target_price_per_m2 NUMERIC,
  refreshed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

ALTER TABLE warehouse.mart_avm_features
  ADD COLUMN IF NOT EXISTS bedrooms_missing BOOLEAN,
  ADD COLUMN IF NOT EXISTS bedrooms_imputed INTEGER,
  ADD COLUMN IF NOT EXISTS bathrooms_missing BOOLEAN,
  ADD COLUMN IF NOT EXISTS bathrooms_imputed INTEGER,
  ADD COLUMN IF NOT EXISTS floors_missing BOOLEAN,
  ADD COLUMN IF NOT EXISTS floors_imputed INTEGER,
  ADD COLUMN IF NOT EXISTS is_outlier_price BOOLEAN,
  ADD COLUMN IF NOT EXISTS is_outlier_area BOOLEAN,
  ADD COLUMN IF NOT EXISTS is_outlier_price_per_m2 BOOLEAN,
  ADD COLUMN IF NOT EXISTS is_outlier_any BOOLEAN,
  ADD COLUMN IF NOT EXISTS is_robust_train_candidate BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_mart_market_analytics_geo
  ON warehouse.mart_market_analytics (province, district);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_location_id
  ON warehouse.mart_avm_features (location_id);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_time_bucket
  ON warehouse.mart_avm_features (time_bucket);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_is_outlier_any
  ON warehouse.mart_avm_features (is_outlier_any);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_robust_candidate
  ON warehouse.mart_avm_features (is_robust_train_candidate);

COMMIT;
