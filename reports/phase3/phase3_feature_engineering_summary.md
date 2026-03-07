# Phase 3 Feature Engineering Summary

## 1) Final Output Dataset

- Table: `warehouse.mart_avm_features_final`
- Row count: **49,955**
- Robust-train candidate rows: **42,303**
- Area-outlier rows: **4,254**

## 2) Reused Decisions From Phase 2

- Bedrooms, bathrooms, and floors reuse the Phase 2 location-aware median imputation strategy.
- Outlier flags are reused, but target-derived flags are now explicitly marked as analysis/filtering only.
- `frontage` is reinterpreted as a boolean presence signal because the raw CSV stores it as true/false.

## 3) Target Transformation

- Model target: `target_log_price_million_vnd`
- Raw target retained: `target_price_million_vnd`
- Rationale: Phase 2 showed strong right skew in raw price and materially improved shape after log transform.
- Recommendation: Train regression models on log1p(price_million_vnd) and exponentiate predictions back to raw price for reporting.

## 4) Numeric Feature Strategy

- `area_m2`: keep_raw_plus_log_plus_cap - Highly skewed with extreme tail; tree models can use raw+cap, linear models benefit from log.
- `bedrooms_imputed`: keep_raw_plus_cap - Count feature with moderate signal; no log transform needed.
- `bathrooms_imputed`: keep_raw_plus_cap - Count feature with strongest direct numeric relation to price in Phase 2.
- `floors_imputed`: keep_raw_plus_cap - Need capping because extreme values remain in source data.
- `timeline_hours`: keep_raw_plus_log_plus_bucket - Freshness effect is nonlinear and heavily right-skewed.
- `frontage`: convert_to_has_frontage - Current source behaves like a boolean presence flag, not frontage width.

## 5) Time Features

- `timeline_hours_imputed` for AVM and analytics: Continuous freshness feature with median fallback for any missing rows.
- `timeline_log_hours` for AVM: Compresses long-tail listing age.
- `timeline_bucket` for AVM and analytics: Stable categorical buckets for recency segmentation.
- `is_new_listing` for AVM and analytics: Simple binary freshness signal for first 72 hours.
- `is_stale_listing` for AVM and analytics: Separates long-tail inventory older than 30 days.

## 6) Location Encoding Guidance

- Tree models: Keep province, district, and timeline_bucket as categorical columns. Add train-only market aggregate and frequency features.
- CatBoost: Use raw string categoricals directly. Do not one-hot district manually.
- LightGBM: Use pandas category dtype or integer-coded categories plus frequency features; scaling is not required.
- Linear models: Use one-hot encoding for province, district, and timeline_bucket. Prefer log/capped numeric columns and scale continuous features in the model pipeline.

## 7) Leakage-safe Market Aggregate Strategy

- Market aggregates: Province/district medians and frequency encodings must be fit on the training split only, then merged into validation/test rows.
- OOF training features: When training with cross-validation, build province/district aggregate features out-of-fold for the training rows instead of fitting them once on the full training fold.
- Target-derived outlier flags: Do not use is_outlier_price, is_outlier_price_per_m2, is_outlier_any, or is_robust_train_candidate as predictors because they use target information.
- Warehouse table semantics: warehouse.mart_avm_features_final stores full-data reference aggregates for inspection and serving refresh, not for leakage-safe offline validation.

## 8) Final Feature Selection

### Required features

- `area_m2`
- `area_m2_capped_p99`
- `log_area_m2`
- `bedrooms_imputed`
- `bedrooms_missing`
- `bathrooms_imputed`
- `bathrooms_missing`
- `floors_imputed`
- `floors_missing`
- `has_frontage`
- `timeline_hours_imputed`
- `timeline_log_hours`
- `timeline_bucket`
- `is_new_listing`
- `province`
- `district`
- `total_rooms`
- `area_per_room`
- `bedroom_density`
- `bathroom_density`
- `is_large_property`
- `is_multi_floor`

### Optional features

- `province_frequency_ref`
- `district_frequency_ref`
- `province_listing_count_ref`
- `district_listing_count_ref`
- `province_median_price_million_vnd_ref`
- `province_median_price_per_m2_ref`
- `district_median_price_million_vnd_ref`
- `district_median_price_per_m2_ref`
- `district_to_province_price_ratio_ref`
- `district_to_province_price_per_m2_ratio_ref`
- `timeline_hours_capped_p99`
- `bedrooms_capped_p99`
- `bathrooms_capped_p99`
- `floors_capped_p99`
- `has_frontage_x_log_area`
- `is_outlier_area`

### Excluded from predictors

- `bedrooms`
- `bathrooms`
- `floors`
- `frontage_raw`
- `target_price_per_m2`
- `target_log_price_per_m2`
- `is_outlier_price`
- `is_outlier_price_per_m2`
- `is_outlier_any`
- `is_robust_train_candidate`

## 9) Generated Artifacts

- Input feature review: `reports/phase3/tables/input_feature_review.csv`
- Numeric transform decisions: `reports/phase3/tables/numeric_transform_decisions.csv`
- Time feature decisions: `reports/phase3/tables/time_feature_decisions.csv`
- Final feature selection: `reports/phase3/tables/final_feature_selection.csv`

## 10) Validation Snapshot

- `timeline_missing_rows`: **0**
- `frontage_missing_rows`: **0**
- `bedrooms_missing_rows`: **5,418**
- `bathrooms_missing_rows`: **8,191**
- `floors_missing_rows`: **10,898**
- `price_outlier_rows`: **4,675**
- `price_per_m2_outlier_rows`: **2,476**

## 11) Best Practices For Phase 4

- Split train/validation before fitting frequency or market aggregate encoders.
- Train on `target_log_price_million_vnd`, then invert with `expm1` for business reporting.
- For CatBoost, keep raw categoricals and avoid manual one-hot encoding for `district`.
- For linear baselines, use one-hot province/district/timeline_bucket and scale continuous variables outside the warehouse.
