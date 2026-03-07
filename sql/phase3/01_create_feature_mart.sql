BEGIN;

CREATE TABLE IF NOT EXISTS warehouse.mart_avm_features_final (
  listing_id TEXT PRIMARY KEY,
  location_id INTEGER,
  province TEXT,
  district TEXT,
  province_listing_count_ref INTEGER,
  district_listing_count_ref INTEGER,
  province_frequency_ref NUMERIC,
  district_frequency_ref NUMERIC,
  province_median_price_million_vnd_ref NUMERIC,
  province_median_price_per_m2_ref NUMERIC,
  district_median_price_million_vnd_ref NUMERIC,
  district_median_price_per_m2_ref NUMERIC,
  district_to_province_price_ratio_ref NUMERIC,
  district_to_province_price_per_m2_ratio_ref NUMERIC,
  timeline_hours INTEGER,
  timeline_missing BOOLEAN,
  timeline_hours_imputed INTEGER,
  timeline_hours_capped_p99 NUMERIC,
  timeline_days_imputed NUMERIC,
  timeline_log_hours NUMERIC,
  timeline_bucket TEXT,
  is_new_listing BOOLEAN,
  is_stale_listing BOOLEAN,
  area_m2 NUMERIC,
  area_m2_capped_p99 NUMERIC,
  log_area_m2 NUMERIC,
  bedrooms_imputed INTEGER,
  bedrooms_missing BOOLEAN,
  bedrooms_capped_p99 NUMERIC,
  bathrooms_imputed INTEGER,
  bathrooms_missing BOOLEAN,
  bathrooms_capped_p99 NUMERIC,
  floors_imputed INTEGER,
  floors_missing BOOLEAN,
  floors_capped_p99 NUMERIC,
  frontage_raw NUMERIC,
  frontage_missing BOOLEAN,
  has_frontage BOOLEAN,
  total_rooms INTEGER,
  area_per_room NUMERIC,
  bedroom_density NUMERIC,
  bathroom_density NUMERIC,
  has_frontage_x_log_area NUMERIC,
  is_large_property BOOLEAN,
  is_multi_floor BOOLEAN,
  is_outlier_area BOOLEAN,
  is_outlier_price BOOLEAN,
  is_outlier_price_per_m2 BOOLEAN,
  is_outlier_any BOOLEAN,
  is_robust_train_candidate BOOLEAN,
  target_price_million_vnd NUMERIC,
  target_log_price_million_vnd NUMERIC,
  target_price_per_m2 NUMERIC,
  target_log_price_per_m2 NUMERIC,
  feature_version TEXT DEFAULT 'phase3_v1',
  refreshed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_final_location
  ON warehouse.mart_avm_features_final (province, district);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_final_location_id
  ON warehouse.mart_avm_features_final (location_id);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_final_timeline_bucket
  ON warehouse.mart_avm_features_final (timeline_bucket);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_final_robust_candidate
  ON warehouse.mart_avm_features_final (is_robust_train_candidate);

CREATE INDEX IF NOT EXISTS idx_mart_avm_features_final_area_outlier
  ON warehouse.mart_avm_features_final (is_outlier_area);

COMMIT;
