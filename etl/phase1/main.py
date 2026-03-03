#!/usr/bin/env python3
"""Pipeline orchestration for Phase 1 ETL."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine

from .config import parse_args
from .extract import build_raw_landing_dataframe, build_staging_dataframe, read_raw_text_dataframe
from .load import (
    fetch_count,
    load_fact,
    load_raw_landing,
    load_staging,
    refresh_location_dimension,
)
from .quality import QualityThresholds, compute_quality_metrics, enforce_quality_gate
from .run_log import finish_etl_run, start_etl_run
from .sql_runner import run_sql_file
from .transform import build_fact_dataframe


def _build_loggable_config(args) -> dict[str, object]:
    return {
        "csv_path": args.csv_path,
        "sql_dir": args.sql_dir,
        "skip_marts": bool(args.skip_marts),
        "skip_quality_gate": bool(args.skip_quality_gate),
        "thresholds": {
            "max_null_price_ratio": args.max_null_price_ratio,
            "max_invalid_price_ratio": args.max_invalid_price_ratio,
            "max_null_area_ratio": args.max_null_area_ratio,
            "max_invalid_area_ratio": args.max_invalid_area_ratio,
            "max_duplicate_detail_url_ratio": args.max_duplicate_detail_url_ratio,
            "min_fact_row_ratio": args.min_fact_row_ratio,
        },
    }


def main() -> None:
    args = parse_args()
    if not args.db_uri:
        raise SystemExit("Missing DB URI. Set --db-uri or DW_PG_URI.")

    csv_path = Path(args.csv_path)
    sql_dir = Path(args.sql_dir)
    engine = create_engine(args.db_uri, future=True)

    for ddl_file in [
        sql_dir / "01_create_staging.sql",
        sql_dir / "02_create_warehouse_core.sql",
        sql_dir / "04_create_marts.sql",
    ]:
        run_sql_file(engine, ddl_file)

    run_id = start_etl_run(
        engine,
        pipeline_name="phase1_etl",
        config=_build_loggable_config(args),
    )

    row_counts: dict[str, int] = {}
    quality_metrics: dict[str, object] = {}

    try:
        df_raw_text = read_raw_text_dataframe(str(csv_path))
        raw_landing_df = build_raw_landing_dataframe(df_raw_text)
        staging_df = build_staging_dataframe(df_raw_text)
        fact_df = build_fact_dataframe(staging_df)

        row_counts["raw_source_rows"] = int(len(df_raw_text))
        row_counts["raw_landing_rows"] = int(len(raw_landing_df))
        row_counts["staging_rows"] = int(len(staging_df))
        row_counts["fact_rows"] = int(len(fact_df))

        quality_metrics = compute_quality_metrics(staging_df, fact_df)
        thresholds = QualityThresholds(
            max_null_price_ratio=args.max_null_price_ratio,
            max_invalid_price_ratio=args.max_invalid_price_ratio,
            max_null_area_ratio=args.max_null_area_ratio,
            max_invalid_area_ratio=args.max_invalid_area_ratio,
            max_duplicate_detail_url_ratio=args.max_duplicate_detail_url_ratio,
            min_fact_row_ratio=args.min_fact_row_ratio,
        )
        quality_metrics["thresholds"] = {
            "max_null_price_ratio": thresholds.max_null_price_ratio,
            "max_invalid_price_ratio": thresholds.max_invalid_price_ratio,
            "max_null_area_ratio": thresholds.max_null_area_ratio,
            "max_invalid_area_ratio": thresholds.max_invalid_area_ratio,
            "max_duplicate_detail_url_ratio": thresholds.max_duplicate_detail_url_ratio,
            "min_fact_row_ratio": thresholds.min_fact_row_ratio,
        }

        if not args.skip_quality_gate:
            enforce_quality_gate(quality_metrics, thresholds)
            quality_metrics["gate_passed"] = True
        else:
            quality_metrics["gate_passed"] = None
            quality_metrics["gate_skipped"] = True

        load_raw_landing(engine, raw_landing_df)
        load_staging(engine, staging_df)
        load_fact(engine, fact_df)
        refresh_location_dimension(engine)

        row_counts["raw_landing_rows"] = fetch_count(engine, "staging.stg_listings_raw_text")
        row_counts["staging_rows"] = fetch_count(engine, "staging.stg_listings_raw")
        row_counts["fact_rows"] = fetch_count(engine, "warehouse.fact_listings")
        row_counts["dim_location_rows"] = fetch_count(engine, "warehouse.dim_location")

        if not args.skip_marts:
            run_sql_file(engine, sql_dir / "05_refresh_marts.sql")
            row_counts["mart_market_analytics_rows"] = fetch_count(engine, "warehouse.mart_market_analytics")
            row_counts["mart_avm_features_rows"] = fetch_count(engine, "warehouse.mart_avm_features")
        else:
            row_counts["mart_market_analytics_rows"] = 0
            row_counts["mart_avm_features_rows"] = 0

        finish_etl_run(
            engine,
            run_id=run_id,
            status="success",
            row_counts=row_counts,
            quality_metrics=quality_metrics,
        )

        print(f"Phase 1 ETL completed. run_id={run_id}")
        print(f"staging.stg_listings_raw_text: {row_counts['raw_landing_rows']:,}")
        print(f"staging.stg_listings_raw: {row_counts['staging_rows']:,}")
        print(f"warehouse.fact_listings: {row_counts['fact_rows']:,}")
        print(f"warehouse.dim_location: {row_counts['dim_location_rows']:,}")
        if not args.skip_marts:
            print(f"warehouse.mart_market_analytics: {row_counts['mart_market_analytics_rows']:,}")
            print(f"warehouse.mart_avm_features: {row_counts['mart_avm_features_rows']:,}")

    except Exception as exc:  # noqa: BLE001
        quality_metrics.setdefault("gate_passed", False)
        finish_etl_run(
            engine,
            run_id=run_id,
            status="failed",
            row_counts=row_counts,
            quality_metrics=quality_metrics,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
