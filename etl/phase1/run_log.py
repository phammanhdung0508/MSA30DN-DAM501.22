#!/usr/bin/env python3
"""ETL run logging helpers."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text


def start_etl_run(engine, pipeline_name: str, config: dict[str, Any]) -> int:
    sql = text(
        """
    INSERT INTO warehouse.etl_run_log (
      pipeline_name,
      status,
      config
    ) VALUES (
      :pipeline_name,
      'running',
      CAST(:config_json AS jsonb)
    )
    RETURNING run_id;
    """
    )
    with engine.begin() as conn:
        result = conn.execute(
            sql,
            {
                "pipeline_name": pipeline_name,
                "config_json": json.dumps(config, ensure_ascii=False),
            },
        )
        return int(result.scalar_one())


def finish_etl_run(
    engine,
    run_id: int,
    status: str,
    row_counts: dict[str, int],
    quality_metrics: dict[str, Any],
    error_message: str | None = None,
) -> None:
    sql = text(
        """
    UPDATE warehouse.etl_run_log
    SET
      finished_at = NOW(),
      status = :status,
      raw_source_rows = :raw_source_rows,
      raw_landing_rows = :raw_landing_rows,
      staging_rows = :staging_rows,
      fact_rows = :fact_rows,
      dim_location_rows = :dim_location_rows,
      mart_market_analytics_rows = :mart_market_analytics_rows,
      mart_avm_features_rows = :mart_avm_features_rows,
      quality_metrics = CAST(:quality_metrics_json AS jsonb),
      error_message = :error_message
    WHERE run_id = :run_id;
    """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "run_id": run_id,
                "status": status,
                "raw_source_rows": row_counts.get("raw_source_rows"),
                "raw_landing_rows": row_counts.get("raw_landing_rows"),
                "staging_rows": row_counts.get("staging_rows"),
                "fact_rows": row_counts.get("fact_rows"),
                "dim_location_rows": row_counts.get("dim_location_rows"),
                "mart_market_analytics_rows": row_counts.get("mart_market_analytics_rows"),
                "mart_avm_features_rows": row_counts.get("mart_avm_features_rows"),
                "quality_metrics_json": json.dumps(quality_metrics, ensure_ascii=False),
                "error_message": error_message,
            },
        )
