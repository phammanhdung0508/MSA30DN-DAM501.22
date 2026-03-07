# DAM501.22 Final - Real Estate DW (Phase 1-3)

Data Warehouse Phase 1-3 for Vietnam real-estate buying listings.

## Project Goal
- Prepare clean warehouse data for AVM (price prediction).
- Prepare market analytics mart (price trend/heatmap by location and time bucket).

## Dataset
- Source file: `data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv`
- Columns: `id, detail_url, title, location, timeline_hours, area_m2, bedrooms, bathrooms, floors, frontage, price_million_vnd`

## Tech Stack
- PostgreSQL
- Python + Pandas + SQLAlchemy + psycopg2
- Jupyter notebook for profiling

## Current Deliverables
- `staging.stg_listings_raw_text` (raw landing, all text)
- `staging.stg_listings_raw`
- `warehouse.fact_listings`
- `warehouse.dim_location`
- `warehouse.mart_market_analytics`
- `warehouse.mart_avm_features` (including imputed feature columns, missing flags, and outlier flags for AVM)
- `warehouse.mart_avm_features_final` (Phase 3 feature-engineered mart for AVM)
- `warehouse.etl_run_log`
- Optional script for `warehouse.dim_time` (Option B)
- Profiling report (JSON + Markdown)
- Warehouse architecture doc + implementation plan
- Phase 2 EDA SQL pack, notebook, figures, and summary report
- Phase 3 SQL pack, feature-engineering pipeline, feature catalog, and summary report

## Repository Structure
- `etl/phase1_profile.py`: data profiling script
- `etl/phase1_etl.py`: ETL entrypoint (backward compatible)
- `etl/phase1/`: refactored ETL modules
- `etl/phase2_eda.py`: Phase 2 EDA entrypoint (backward compatible)
- `etl/phase2/`: refactored Phase 2 EDA modules
- `etl/phase3_feature_engineering.py`: Phase 3 feature engineering entrypoint (backward compatible)
- `etl/phase3/`: refactored Phase 3 feature engineering modules
- `sql/phase1/`: DDL/DML/validation SQL scripts
- `sql/phase2/`: EDA analysis SQL scripts
- `sql/phase3/`: feature mart DDL/DML/validation SQL scripts
- `reports/`: generated profiling outputs
- `docs/`: architecture and implementation docs
- `notebooks/`: profiling + EDA notebooks

## ETL Module Split
Core ETL logic is split to keep responsibilities clear:
- `etl/phase1/config.py`
- `etl/phase1/extract.py`
- `etl/phase1/transform.py`
- `etl/phase1/load.py`
- `etl/phase1/quality.py`
- `etl/phase1/run_log.py`
- `etl/phase1/sql_runner.py`
- `etl/phase1/main.py`

`etl/phase1_etl.py` only calls `phase1.main.main()`.

## EDA Module Split (Phase 2)
Core EDA logic is split for readability:
- `etl/phase2/config.py`
- `etl/phase2/constants.py`
- `etl/phase2/extract.py`
- `etl/phase2/analysis_quality.py`
- `etl/phase2/analysis_target.py`
- `etl/phase2/analysis_market.py`
- `etl/phase2/analysis_features.py`
- `etl/phase2/reporting.py`
- `etl/phase2/main.py`

`etl/phase2_eda.py` only calls `phase2.main.main()`.

## Feature Engineering Module Split (Phase 3)
Core Phase 3 logic is split for readability:
- `etl/phase3/config.py`
- `etl/phase3/constants.py`
- `etl/phase3/extract.py`
- `etl/phase3/modeling.py`
- `etl/phase3/paths.py`
- `etl/phase3/reporting.py`
- `etl/phase3/main.py`

`etl/phase3_feature_engineering.py` only calls `phase3.main.main()`.

## Setup
Use Conda env `p31114`.

```bash
conda run -n p31114 python -m pip install -r requirements.txt
```

## PostgreSQL Connection
Quick connection test to database `DAM501.22`:

```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' -c '\conninfo'
```

ETL DB URI:

```bash
export DW_PG_URI='postgresql+psycopg2://postgres:postgres@localhost:5432/DAM501.22'
```

## Run Pipeline (Manual)
### 1) Profiling
```bash
conda run --no-capture-output -n p31114 python etl/phase1_profile.py \
  --input-csv data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv \
  --output-json reports/phase1_profiling_summary.json \
  --output-md reports/phase1_profiling_summary.md
```

### 2) ETL to Warehouse
```bash
conda run --no-capture-output -n p31114 python etl/phase1_etl.py \
  --db-uri "$DW_PG_URI" \
  --csv-path data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv \
  --sql-dir sql/phase1
```

Quality gate options (default enabled):
- `--max-null-price-ratio` (default `0.10`)
- `--max-invalid-price-ratio` (default `0.02`)
- `--max-null-area-ratio` (default `0.35`)
- `--max-invalid-area-ratio` (default `0.01`)
- `--max-duplicate-detail-url-ratio` (default `0.01`)
- `--min-fact-row-ratio` (default `0.60`)
- `--skip-quality-gate` (manual override)

### 3) Validation
```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -f sql/phase1/06_validation_checks.sql
```

## Run Phase 2 EDA (Manual)
### 1) SQL analysis pack
```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -f sql/phase2/01_eda_queries.sql
```

### 2) Python EDA runner (charts + tables + summary)
```bash
conda run --no-capture-output -n p31114 python etl/phase2_eda.py \
  --db-uri "$DW_PG_URI" \
  --output-dir reports/phase2
```

### 3) Open EDA notebook (interactive)
- Notebook file: `notebooks/phase2_eda_fact_listings.ipynb`
- Notebook covers quality, target distribution, location analysis, size/feature analysis, correlation, timeline, mart validation, and AVM feature shortlist.

### 4) Phase 2 outputs
- Summary markdown: `reports/phase2/phase2_eda_summary.md`
- Summary JSON: `reports/phase2/phase2_eda_summary.json`
- Tables: `reports/phase2/tables/*.csv`
- Figures: `reports/phase2/figures/*.png`

## Run Phase 3 Feature Engineering (Manual)
### 1) Build the final AVM feature mart
```bash
conda run --no-capture-output -n p31114 python etl/phase3_feature_engineering.py \
  --db-uri "$DW_PG_URI" \
  --sql-dir sql/phase3 \
  --output-dir reports/phase3
```

### 2) Validation
```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -f sql/phase3/03_validation_checks.sql
```

### 3) Phase 3 outputs
- Final feature mart: `warehouse.mart_avm_features_final`
- Summary markdown: `reports/phase3/phase3_feature_engineering_summary.md`
- Summary JSON: `reports/phase3/phase3_feature_engineering_summary.json`
- Tables: `reports/phase3/tables/*.csv`

## SQL Execution Order (SQL-only path)
Python ETL (`etl/phase1_etl.py`) is the canonical transform path.
`sql/phase1/03_transform_staging_to_fact.sql` is kept only as a legacy/manual fallback.
Legacy SQL transform is blocked by default and requires explicit opt-in:
- pass `-v allow_legacy_sql_transform=1`
- quality gate and `etl_run_log` are not applied in this path

1. `sql/phase1/01_create_staging.sql`
2. `sql/phase1/01b_load_staging_from_csv.sql`
3. `sql/phase1/02_create_warehouse_core.sql`
4. `sql/phase1/03_transform_staging_to_fact.sql` (legacy fallback, not canonical)
5. `sql/phase1/04_create_marts.sql`
6. `sql/phase1/05_refresh_marts.sql`
7. `sql/phase1/06_validation_checks.sql`

For step 2 (`01b_load_staging_from_csv.sql`), pass CSV path variable:

```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -v csv_path='data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv' \
  -f sql/phase1/01b_load_staging_from_csv.sql
```

If your DB was created before the latest update, run these first to create new tables:

```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' -f sql/phase1/01_create_staging.sql
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' -f sql/phase1/02_create_warehouse_core.sql
```

## Cleaning Logic Implemented
- Remove invalid `price_million_vnd <= 0`.
- Remove invalid `area_m2 <= 0`.
- Deduplicate by `detail_url` (fallback to `id` when URL missing).
- Fail fast if duplicate `listing_id` still exists after deduplication (no silent row drop).
- Normalize location into `province` and `district` (accent removal + prefix normalization).
- Fallback location to `Unknown` when parsing fails.
- Compute `price_per_m2 = price_million_vnd / area_m2`.
- Time handling uses Option A: `timeline_hours` + `time_bucket`.
- AVM mart imputes `bedrooms`, `bathrooms`, and `floors` with location-aware median fallback:
  - district median when at least 30 observed values exist
  - else province median when at least 30 observed values exist
  - else global median
- AVM mart adds missing flags: `bedrooms_missing`, `bathrooms_missing`, `floors_missing`.
- AVM mart adds modeling columns: `bedrooms_imputed`, `bathrooms_imputed`, `floors_imputed`.
- AVM mart adds IQR-based outlier flags: `is_outlier_price`, `is_outlier_area`, `is_outlier_price_per_m2`, `is_outlier_any`.
- AVM mart adds `is_robust_train_candidate` for outlier-filtered training experiments.
- Phase 3 final mart adds:
  - transformed target: `target_log_price_million_vnd`
  - transformed numeric features: `log_area_m2`, capped numeric variants, and density/room interactions
  - time features: `timeline_hours_imputed`, `timeline_log_hours`, `timeline_bucket`, `is_new_listing`, `is_stale_listing`
  - location reference features: province/district counts, frequencies, medians, and district-to-province ratios
  - frontage reinterpretation: `has_frontage`

## Important Notes
- ETL is full refresh for raw landing/staging/fact/marts using atomic table-replace steps.
- Missing `bedrooms`, `bathrooms`, and `floors` are not overwritten in `warehouse.fact_listings`; imputation happens only in `warehouse.mart_avm_features`.
- Outliers are not removed from `warehouse.fact_listings`; they are flagged in `warehouse.mart_avm_features` for downstream modeling.
- Recommended AVM training policy:
  - base training table: `warehouse.mart_avm_features_final`
  - model target: `target_log_price_million_vnd`
  - reuse `province`, `district`, and `timeline_bucket` as categorical features for tree-based models
  - refit frequency and market aggregate encoders on training rows only; use out-of-fold aggregates for training rows and do not use the warehouse `_ref` columns directly for offline validation
  - do not use `is_outlier_price`, `is_outlier_price_per_m2`, `is_outlier_any`, or `is_robust_train_candidate` as predictors because they are target-derived or target-dependent
  - optional robust experiment: filter `is_robust_train_candidate = true`
- ETL writes run metadata into `warehouse.etl_run_log` (status, row counts, quality metrics, error message).

## Quick Verification
Check latest ETL runs:

```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -c "SELECT run_id, status, started_at, finished_at, raw_source_rows, raw_landing_rows, staging_rows, fact_rows, mart_market_analytics_rows, mart_avm_features_rows FROM warehouse.etl_run_log ORDER BY run_id DESC LIMIT 5;"
```

Check current row counts:

```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -c "SELECT 'staging_raw_text' AS table_name, COUNT(*) FROM staging.stg_listings_raw_text UNION ALL SELECT 'staging_raw', COUNT(*) FROM staging.stg_listings_raw UNION ALL SELECT 'fact', COUNT(*) FROM warehouse.fact_listings UNION ALL SELECT 'mart_market_analytics', COUNT(*) FROM warehouse.mart_market_analytics UNION ALL SELECT 'mart_avm_features', COUNT(*) FROM warehouse.mart_avm_features;"
```

Check Phase 3 row counts:

```bash
export PGPASSWORD='postgres'
psql -h localhost -p 5432 -U postgres -d 'DAM501.22' \
  -c "SELECT 'mart_avm_features' AS table_name, COUNT(*) FROM warehouse.mart_avm_features UNION ALL SELECT 'mart_avm_features_final', COUNT(*) FROM warehouse.mart_avm_features_final;"
```

## Run Unit Tests
```bash
cd /home/sunf/FSB/DAM501.22/Final
conda run --no-capture-output -n p31114 python -m unittest discover -s tests -p 'test_*.py'
```

## Documents
- Architecture: `docs/phase1_warehouse_architecture.md`
- Plan: `docs/phase1_implementation_plan.md`
- Phase 2 plan: `docs/phase2_eda_plan.md`
- Phase 3 plan: `docs/phase3_feature_engineering_plan.md`
- Profiling summary: `reports/phase1_profiling_summary.md`
- Profiling detail JSON: `reports/phase1_profiling_summary.json`
- Notebook: `notebooks/phase1_data_profiling.ipynb`
- Phase 2 notebook: `notebooks/phase2_eda_fact_listings.ipynb`
- Phase 2 summary: `reports/phase2/phase2_eda_summary.md`
- Phase 3 summary: `reports/phase3/phase3_feature_engineering_summary.md`
