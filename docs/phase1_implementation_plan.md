# Phase 1 Implementation Plan

## 1) Environment Setup
1. Create/activate Python environment (`p31114`).
2. Install dependencies in `requirements-phase1.txt`.
3. Prepare PostgreSQL database and connection URI (`DW_PG_URI`).
4. Verify CSV exists at:
   - `data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv`

## 2) Staging Layer
1. Run `sql/phase1/01_create_staging.sql`.
2. Load CSV to `staging.stg_listings_raw_text` first (all text, raw fidelity).
3. Build typed staging data in `staging.stg_listings_raw`.
4. Use either:
   - Python path: `etl/phase1_etl.py` (recommended)
   - SQL path: `sql/phase1/01b_load_staging_from_csv.sql`

## 3) Data Profiling
1. Run `etl/phase1_profile.py`.
2. Review outputs:
   - `reports/phase1_profiling_summary.json`
   - `reports/phase1_profiling_summary.md`
3. Confirm cleaning thresholds and outlier strategy.

## 4) Core Warehouse Build
1. Run `sql/phase1/02_create_warehouse_core.sql`.
2. Evaluate quality gate thresholds before loading fact tables.
3. Build cleaned fact with ETL script (recommended) or SQL transform script:
   - Python: `etl/phase1_etl.py`
   - SQL fallback: `sql/phase1/03_transform_staging_to_fact.sql`
4. Populate `warehouse.dim_location` from distinct `(province, district)` pairs.
5. Write run metadata to `warehouse.etl_run_log`.

## 5) Time Handling
- Option selected in Phase 1: **Option A**.
- Keep `timeline_hours` as feature and derive `time_bucket` for marts.
- Optional Option B template is prepared in `sql/phase1/07_optional_dim_time.sql`.

## 6) Marts
1. Run `sql/phase1/04_create_marts.sql`.
2. Run `sql/phase1/05_refresh_marts.sql`.
3. Verify:
   - `warehouse.mart_market_analytics`
   - `warehouse.mart_avm_features`

## 7) Validation
Run `sql/phase1/06_validation_checks.sql` and confirm:
1. No invalid price/area in fact table.
2. No duplicate dedupe keys in fact table.
3. `price_per_m2` is consistent.
4. AVM mart rows have `location_id` coverage.

## 8) Artifacts
- SQL scripts: `sql/phase1/*`
- ETL scripts: `etl/phase1_etl.py`, `etl/phase1_profile.py`
- ETL modules: `etl/phase1/*`
- Profiling report: `reports/phase1_profiling_summary.*`
- Architecture diagram: `docs/phase1_warehouse_architecture.md`
- Profiling notebook: `notebooks/phase1_data_profiling.ipynb`
