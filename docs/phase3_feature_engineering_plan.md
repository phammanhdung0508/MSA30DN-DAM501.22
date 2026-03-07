# Phase 3 Feature Engineering Plan

## Scope
Phase 3 transforms `warehouse.mart_avm_features` into a model-oriented dataset for AVM while keeping the warehouse reusable for market analytics.

Primary output:
- `warehouse.mart_avm_features_final`

Secondary outputs:
- `reports/phase3/phase3_feature_engineering_summary.md`
- `reports/phase3/phase3_feature_engineering_summary.json`
- `reports/phase3/tables/*.csv`

## What Is Reused From Phase 2
1. `bedrooms`, `bathrooms`, and `floors` keep the Phase 2 location-aware median imputation strategy.
2. Missing flags from Phase 2 are reused directly.
3. Existing outlier flags are reused, but their modeling role is tightened:
   - `is_outlier_area` may be used as an optional predictor.
   - `is_outlier_price`, `is_outlier_price_per_m2`, `is_outlier_any`, and `is_robust_train_candidate` are not predictors because they depend on target-derived rules.
4. `timeline_hours` continues to use Option A (feature engineering only, no synthetic timestamp).

## Step-by-step Workflow
1. Start from `warehouse.mart_avm_features`.
2. Review source columns and classify them into:
   - numeric
   - categorical
   - derived
   - target
3. Build transformed target columns:
   - `target_log_price_million_vnd = ln(1 + target_price_million_vnd)`
   - keep raw target for reporting
4. Build transformed numeric features:
   - `log_area_m2`
   - P99-capped versions of area and count features
   - boolean `has_frontage`
5. Build time features:
   - `timeline_hours_imputed`
   - `timeline_log_hours`
   - `timeline_bucket`
   - `is_new_listing`
   - `is_stale_listing`
6. Build location reference features:
   - province/district listing counts
   - province/district frequency references
   - province/district median price references
   - district-to-province price ratios
7. Build interaction features:
   - `total_rooms`
   - `area_per_room`
   - `bedroom_density`
   - `bathroom_density`
   - `has_frontage_x_log_area`
   - `is_large_property`
   - `is_multi_floor`
8. Materialize `warehouse.mart_avm_features_final`.
9. Validate row counts, nulls, and transform consistency.
10. Use Python train-only encoders when training a model to avoid leakage.

## Design Decisions
### 1) Target format
- Recommended target for AVM training: `target_log_price_million_vnd`
- Reason: Phase 2 showed raw price is strongly right-skewed.

### 2) Numeric scaling
- No scaling is materialized in the warehouse.
- Reason: scaling is model-family specific and should happen in the training pipeline, not in shared warehouse storage.

### 3) Frontage handling
- Source `frontage` behaves like a boolean field, not frontage width.
- Phase 3 therefore uses `has_frontage` for modeling and keeps `frontage_raw` only for audit.

### 4) Province and district
- Keep both.
- `province` captures macro market level.
- `district` captures micro location premium, which is typically much stronger for AVM.

### 5) Leakage-safe aggregates
- The warehouse stores full-data reference aggregates with `_ref` suffix.
- Offline training must refit these aggregates using training rows only, then apply them to validation/test rows.
- For training rows themselves, use out-of-fold aggregate generation so each row does not see its own target in the aggregate.
- This logic is implemented in `etl/phase3/modeling.py`.

## Model-family recommendations
### CatBoost
- Keep `province`, `district`, and `timeline_bucket` as raw categorical fields.
- Add train-only market aggregates.
- Do not manually one-hot encode `district`.

### LightGBM
- Use categorical dtype or integer-coded categories.
- Keep log/capped numeric features.
- Add frequency and market aggregate features.

### Linear baselines
- One-hot encode `province`, `district`, and `timeline_bucket`.
- Use log/capped numeric features.
- Apply scaling in the training pipeline.

## Manual Run Commands
```bash
export DW_PG_URI='postgresql+psycopg2://postgres:postgres@localhost:5432/DAM501.22'

conda run --no-capture-output -n p31114 python etl/phase3_feature_engineering.py \
  --db-uri "$DW_PG_URI" \
  --sql-dir sql/phase3 \
  --output-dir reports/phase3

export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -f sql/phase3/03_validation_checks.sql
```
