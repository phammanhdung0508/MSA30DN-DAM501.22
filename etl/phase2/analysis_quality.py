"""Data quality checks for Phase 2 EDA."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .constants import KEY_COLUMNS
from .paths import Paths, save_table


def analyze_data_quality(fact: pd.DataFrame, paths: Paths) -> dict[str, Any]:
  row_count = int(len(fact))
  missing_records = []
  for col in KEY_COLUMNS:
    miss = int(fact[col].isna().sum())
    if fact[col].dtype == object:
      miss += int((fact[col].astype(str).str.strip() == "").sum())
    missing_records.append(
      {
        "column": col,
        "missing_count": miss,
        "missing_pct": round(100.0 * miss / row_count, 4) if row_count else 0.0,
      }
    )
  missing_df = pd.DataFrame(missing_records).sort_values("column")
  save_table(missing_df, paths.tables / "quality_missing_summary.csv")

  range_rows = []
  for col in ["price_million_vnd", "area_m2", "bedrooms", "bathrooms", "floors", "frontage"]:
    s = pd.to_numeric(fact[col], errors="coerce").dropna()
    range_rows.append(
      {
        "column": col,
        "count": int(s.count()),
        "min": float(s.min()) if not s.empty else None,
        "p50": float(s.quantile(0.5)) if not s.empty else None,
        "p95": float(s.quantile(0.95)) if not s.empty else None,
        "p99": float(s.quantile(0.99)) if not s.empty else None,
        "max": float(s.max()) if not s.empty else None,
      }
    )
  numeric_ranges_df = pd.DataFrame(range_rows)
  save_table(numeric_ranges_df, paths.tables / "quality_numeric_ranges.csv")

  dedupe_key = np.where(
    fact["detail_url"].astype(str).str.strip() != "",
    fact["detail_url"].astype(str).str.strip(),
    "id:" + fact["listing_id"].astype(str),
  )

  duplicate_listing_id = int(fact.duplicated(subset=["listing_id"]).sum())
  duplicate_dedupe_key = int(pd.Series(dedupe_key).duplicated().sum())

  return {
    "row_count": row_count,
    "missing_table_csv": str(paths.tables / "quality_missing_summary.csv"),
    "numeric_ranges_csv": str(paths.tables / "quality_numeric_ranges.csv"),
    "duplicate_listing_id_rows": duplicate_listing_id,
    "duplicate_dedupe_key_rows": duplicate_dedupe_key,
  }
