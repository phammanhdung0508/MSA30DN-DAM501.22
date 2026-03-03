# Phase 1 Warehouse Architecture

## Scope
- Objective: prepare buying-listing data for AVM and market analytics.
- Dataset: `house_buying_dec29th_2025.csv`.
- Time strategy: Option A (`timeline_hours` as feature + bucketization).

## Logical Flow
```mermaid
flowchart LR
    A[Raw CSV\n(data/raw/.../house_buying_dec29th_2025.csv)] --> B[Python ETL\netl/phase1_etl.py]
    B --> C[(staging.stg_listings_raw_text)]
    C --> D[(staging.stg_listings_raw)]
    D --> E[Quality Gate]
    E --> F[Cleaning + Dedup +\nLocation Normalization]
    F --> G[(warehouse.fact_listings)]
    B --> K[(warehouse.etl_run_log)]
    G --> H[(warehouse.dim_location)]
    G --> I[(warehouse.mart_market_analytics)]
    G --> J[(warehouse.mart_avm_features)]
    H --> J
```

## Entities
- `staging.stg_listings_raw_text`
  - Raw landing table from CSV, all columns as text.
  - Keeps source fidelity for audit/reproducibility.
- `staging.stg_listings_raw`
  - Typed staging table.
  - Harmonizes data types before business cleaning.
- `warehouse.fact_listings`
  - Cleaned listing-level fact table.
  - Deduped and enriched with `price_per_m2`, `province`, `district`.
  - Fallback location value: `Unknown` when province/district cannot be parsed.
- `warehouse.dim_location`
  - Unique `(province, district)` pairs.
- `warehouse.mart_market_analytics`
  - OLAP aggregate by `(province, district, time_bucket)`.
- `warehouse.mart_avm_features`
  - Training-ready feature mart with location key and district median features.
- `warehouse.etl_run_log`
  - Run-level metadata, row counts, quality metrics, error details.

## Cleaning Rules
- Drop rows where `price_million_vnd <= 0`.
- Drop rows where `area_m2 <= 0`.
- Compute `price_per_m2 = price_million_vnd / area_m2`.
- Deduplicate by `detail_url`; fallback to `id` when URL is missing.
- Keep the row with smaller `timeline_hours` first, then richer completeness.
- Normalize location:
  - remove accents,
  - normalize admin prefixes,
  - extract `province` and `district`.
  - fallback to `Unknown` for unparsable location fields.

## Time Handling Decision
- Chosen: Option A.
- Rationale:
  - No true timestamp in source.
  - Avoid synthetic `posted_at` assumptions for Phase 1.
  - Still supports trend slices via `time_bucket` on `timeline_hours`:
    - `0_24h`, `24_72h`, `3_7d`, `8_30d`, `gt_30d`, `unknown`.

## Optional Extension
- If Option B is later accepted, use `sql/phase1/07_optional_dim_time.sql` and a fixed dataset snapshot date to back-calculate estimated posting dates.
