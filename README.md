# DAM501.22 Final - Real Estate DW (Phase 1)

Data Warehouse Phase 1 for Vietnam real-estate buying listings.

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
- `warehouse.mart_avm_features`
- `warehouse.etl_run_log`
- Optional script for `warehouse.dim_time` (Option B)
- Profiling report (JSON + Markdown)
- Warehouse architecture doc + implementation plan

## Repository Structure
- `etl/phase1_profile.py`: data profiling script
- `etl/phase1_etl.py`: ETL entrypoint (backward compatible)
- `etl/phase1/`: refactored ETL modules
- `sql/phase1/`: DDL/DML/validation SQL scripts
- `reports/`: generated profiling outputs
- `docs/`: architecture and implementation docs
- `notebooks/`: profiling notebook

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

## SQL Execution Order (SQL-only path)
1. `sql/phase1/01_create_staging.sql`
2. `sql/phase1/01b_load_staging_from_csv.sql`
3. `sql/phase1/02_create_warehouse_core.sql`
4. `sql/phase1/03_transform_staging_to_fact.sql`
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
- Normalize location into `province` and `district` (accent removal + prefix normalization).
- Compute `price_per_m2 = price_million_vnd / area_m2`.
- Time handling uses Option A: `timeline_hours` + `time_bucket`.

## Important Notes
- ETL is full refresh (`TRUNCATE` then reload) for raw landing/staging/fact/marts.
- Outliers are profiled and reported; they are not removed by default in current ETL.
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

## Run Unit Tests
```bash
cd /home/sunf/FSB/DAM501.22/Final
conda run --no-capture-output -n p31114 python -m unittest discover -s tests -p 'test_*.py'
```

## Documents
- Architecture: `docs/phase1_warehouse_architecture.md`
- Plan: `docs/phase1_implementation_plan.md`
- Profiling summary: `reports/phase1_profiling_summary.md`
- Profiling detail JSON: `reports/phase1_profiling_summary.json`
- Notebook: `notebooks/phase1_data_profiling.ipynb`
