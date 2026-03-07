"""Feature catalog and modeling guidance for Phase 3."""

from __future__ import annotations

INPUT_FEATURE_REVIEW = [
  {"column": "area_m2", "group": "numeric", "action": "keep_and_transform", "notes": "Keep raw area, add log/capped versions."},
  {"column": "bedrooms", "group": "numeric", "action": "exclude_raw", "notes": "Use bedrooms_imputed plus bedrooms_missing from Phase 2."},
  {"column": "bathrooms", "group": "numeric", "action": "exclude_raw", "notes": "Use bathrooms_imputed plus bathrooms_missing from Phase 2."},
  {"column": "floors", "group": "numeric", "action": "exclude_raw", "notes": "Use floors_imputed plus floors_missing from Phase 2."},
  {"column": "frontage", "group": "numeric", "action": "reinterpret_as_boolean", "notes": "CSV stores frontage as boolean-like flag, so model with has_frontage."},
  {"column": "timeline_hours", "group": "numeric", "action": "keep_and_transform", "notes": "Keep raw timeline_hours, add imputed/log/bucket features."},
  {"column": "province", "group": "categorical", "action": "keep_and_encode", "notes": "Keep raw for CatBoost and encode per model family."},
  {"column": "district", "group": "categorical", "action": "keep_and_encode", "notes": "Keep raw district because micro-location carries strong pricing signal."},
  {"column": "district_median_price_million_vnd", "group": "derived", "action": "reuse_with_leakage_control", "notes": "Useful market context, but must be recomputed from training data during model evaluation."},
  {"column": "district_median_price_per_m2", "group": "derived", "action": "reuse_with_leakage_control", "notes": "Useful market context, but must be recomputed from training data during model evaluation."},
  {"column": "is_outlier_area", "group": "derived", "action": "optional_predictor", "notes": "Safe optional predictor because it is derived from area, not target."},
  {"column": "is_outlier_price", "group": "derived", "action": "analysis_only", "notes": "Target-derived flag. Do not use as predictor."},
  {"column": "is_outlier_price_per_m2", "group": "derived", "action": "analysis_only", "notes": "Target-derived flag. Do not use as predictor."},
  {"column": "is_outlier_any", "group": "derived", "action": "analysis_or_filter_only", "notes": "Composite outlier flag includes target-derived rules; use for filtering experiments only."},
  {"column": "target_price_million_vnd", "group": "target", "action": "keep_target", "notes": "Store raw target for reporting and inverse-transform interpretation."},
]

NUMERIC_TRANSFORMS = [
  {"feature": "area_m2", "decision": "keep_raw_plus_log_plus_cap", "why": "Highly skewed with extreme tail; tree models can use raw+cap, linear models benefit from log."},
  {"feature": "bedrooms_imputed", "decision": "keep_raw_plus_cap", "why": "Count feature with moderate signal; no log transform needed."},
  {"feature": "bathrooms_imputed", "decision": "keep_raw_plus_cap", "why": "Count feature with strongest direct numeric relation to price in Phase 2."},
  {"feature": "floors_imputed", "decision": "keep_raw_plus_cap", "why": "Need capping because extreme values remain in source data."},
  {"feature": "timeline_hours", "decision": "keep_raw_plus_log_plus_bucket", "why": "Freshness effect is nonlinear and heavily right-skewed."},
  {"feature": "frontage", "decision": "convert_to_has_frontage", "why": "Current source behaves like a boolean presence flag, not frontage width."},
]

TIME_FEATURES = [
  {"feature": "timeline_hours_imputed", "use_for": "AVM and analytics", "why": "Continuous freshness feature with median fallback for any missing rows."},
  {"feature": "timeline_log_hours", "use_for": "AVM", "why": "Compresses long-tail listing age."},
  {"feature": "timeline_bucket", "use_for": "AVM and analytics", "why": "Stable categorical buckets for recency segmentation."},
  {"feature": "is_new_listing", "use_for": "AVM and analytics", "why": "Simple binary freshness signal for first 72 hours."},
  {"feature": "is_stale_listing", "use_for": "AVM and analytics", "why": "Separates long-tail inventory older than 30 days."},
]

LOCATION_ENCODING_GUIDANCE = {
  "tree_models": {
    "recommended": "Keep province, district, and timeline_bucket as categorical columns. Add train-only market aggregate and frequency features.",
    "catboost": "Use raw string categoricals directly. Do not one-hot district manually.",
    "lightgbm": "Use pandas category dtype or integer-coded categories plus frequency features; scaling is not required.",
  },
  "linear_models": {
    "recommended": "Use one-hot encoding for province, district, and timeline_bucket. Prefer log/capped numeric columns and scale continuous features in the model pipeline.",
  },
}

LEAKAGE_GUIDANCE = {
  "market_aggregates": "Province/district medians and frequency encodings must be fit on the training split only, then merged into validation/test rows.",
  "oof_training_features": "When training with cross-validation, build province/district aggregate features out-of-fold for the training rows instead of fitting them once on the full training fold.",
  "target_outlier_flags": "Do not use is_outlier_price, is_outlier_price_per_m2, is_outlier_any, or is_robust_train_candidate as predictors because they use target information.",
  "warehouse_table": "warehouse.mart_avm_features_final stores full-data reference aggregates for inspection and serving refresh, not for leakage-safe offline validation.",
}

FINAL_REQUIRED_FEATURES = [
  "area_m2",
  "area_m2_capped_p99",
  "log_area_m2",
  "bedrooms_imputed",
  "bedrooms_missing",
  "bathrooms_imputed",
  "bathrooms_missing",
  "floors_imputed",
  "floors_missing",
  "has_frontage",
  "timeline_hours_imputed",
  "timeline_log_hours",
  "timeline_bucket",
  "is_new_listing",
  "province",
  "district",
  "total_rooms",
  "area_per_room",
  "bedroom_density",
  "bathroom_density",
  "is_large_property",
  "is_multi_floor",
]

FINAL_OPTIONAL_FEATURES = [
  "province_frequency_ref",
  "district_frequency_ref",
  "province_listing_count_ref",
  "district_listing_count_ref",
  "province_median_price_million_vnd_ref",
  "province_median_price_per_m2_ref",
  "district_median_price_million_vnd_ref",
  "district_median_price_per_m2_ref",
  "district_to_province_price_ratio_ref",
  "district_to_province_price_per_m2_ratio_ref",
  "timeline_hours_capped_p99",
  "bedrooms_capped_p99",
  "bathrooms_capped_p99",
  "floors_capped_p99",
  "has_frontage_x_log_area",
  "is_outlier_area",
]

FINAL_EXCLUDED_FEATURES = [
  "bedrooms",
  "bathrooms",
  "floors",
  "frontage_raw",
  "target_price_per_m2",
  "target_log_price_per_m2",
  "is_outlier_price",
  "is_outlier_price_per_m2",
  "is_outlier_any",
  "is_robust_train_candidate",
]

TARGET_DECISION = {
  "transformed_target": "target_log_price_million_vnd",
  "raw_target": "target_price_million_vnd",
  "recommendation": "Train regression models on log1p(price_million_vnd) and exponentiate predictions back to raw price for reporting.",
  "reason": "Phase 2 showed strong right skew in raw price and materially improved shape after log transform.",
}
