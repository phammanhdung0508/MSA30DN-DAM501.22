# Phase 2 EDA Implementation Plan

## Scope
Phase 2 focuses on exploratory analysis for:
- AVM feature understanding
- Market analytics insight validation

Data source:
- `warehouse.fact_listings`
- `warehouse.mart_market_analytics` (cross-check only)

## Step-by-step Workflow
1. Validate data quality in `warehouse.fact_listings`:
   - row count
   - missing values in key columns
   - numeric ranges
   - duplicate checks
2. Analyze target distributions:
   - `price_million_vnd`
   - `log(price_million_vnd + 1)`
   - `price_per_m2`
3. Analyze location effects:
   - listing count by province
   - median price by province
   - median `price_per_m2` by province and district
4. Analyze property size and price relationship:
   - scatter `area_m2` vs `price_million_vnd`
   - scatter `area_m2` vs `price_per_m2`
   - outlier candidate extraction
5. Analyze property feature effects:
   - boxplot price vs bedrooms
   - boxplot price vs floors
   - distribution of bathrooms/frontage
6. Correlation analysis:
   - numeric correlation matrix
   - detect strong predictors
   - detect multicollinearity
7. Timeline analysis with buckets:
   - `0_24h`, `1_3d`, `3_7d`, `7_30d`, `gt_30d`, `unknown`
8. Validate market analytics mart consistency:
   - rebuild aggregates from fact table
   - compare with `warehouse.mart_market_analytics`
9. Finalize AVM feature shortlist with rationale.
10. Surface engineered outlier flags from `warehouse.mart_avm_features` for Phase 3.
11. Finalize missing-value and outlier handling decisions for modeling.

## Artifacts Created
- SQL query pack: `sql/phase2/01_eda_queries.sql`
- EDA entrypoint: `etl/phase2_eda.py`
- Refactored EDA modules under `etl/phase2/`
- Notebook: `notebooks/phase2_eda_fact_listings.ipynb`
- Output report:
  - `reports/phase2/phase2_eda_summary.md`
  - `reports/phase2/phase2_eda_summary.json`
  - `reports/phase2/figures/*.png`
  - `reports/phase2/tables/*.csv`

## Design Decisions
1. EDA is separated from ETL:
   - avoids changing warehouse data during analysis
   - keeps Phase 2 fully reproducible and read-only
2. SQL + Python dual path:
   - SQL for transparent warehouse-level checks
   - Python for richer visual analysis and consolidated reporting
3. Timeline strategy:
   - uses `timeline_hours` buckets (no synthetic timestamp creation)
   - aligns with current project constraint (no real posted datetime)
4. Robust district ranking:
   - district table applies minimum listing threshold to reduce noise
5. Mart validation uses exact bucket mapping from Phase 1 mart logic (`0_24h`, `24_72h`, `3_7d`, `8_30d`, `gt_30d`, `unknown`).
6. Outlier handling keeps fact data unchanged and pushes flags into `warehouse.mart_avm_features` for reusable downstream features.
7. Missing `bedrooms`, `bathrooms`, and `floors` are handled in `warehouse.mart_avm_features` with location-aware median imputation plus explicit missing flags.

## Current Key Findings Snapshot
- `fact_listings` row count: 49,955
- No duplicate `listing_id` or dedupe key detected.
- Price distribution is strongly right-skewed; log transform improves shape.
- Bedrooms and bathrooms show the strongest direct numeric correlation with total price among current variables.
- `bathrooms` and `bedrooms` are highly correlated with each other (multicollinearity risk).
- Market mart cross-check currently reports zero mismatched groups.
- Modeling decision:
  - use `bedrooms_imputed`, `bathrooms_imputed`, `floors_imputed` plus `*_missing` flags
  - keep outliers in mart and use outlier flags as features
  - compare against robust subset `is_robust_train_candidate = true`

## Manual Run Commands
```bash
export DW_PG_URI='postgresql+psycopg2://postgres:postgres@localhost:5432/DAM501.22'

# SQL EDA checks
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' -f sql/phase2/01_eda_queries.sql

# Python EDA report + figures
conda run --no-capture-output -n p31114 python etl/phase2_eda.py \
  --db-uri "$DW_PG_URI" \
  --output-dir reports/phase2
```
