"""DB extraction helpers for Phase 3."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

FINAL_MART_QUERY = """
SELECT *
FROM warehouse.mart_avm_features_final
ORDER BY listing_id
"""

SUMMARY_QUERY = """
SELECT
  COUNT(*) AS row_count,
  COUNT(*) FILTER (WHERE timeline_missing) AS timeline_missing_rows,
  COUNT(*) FILTER (WHERE frontage_missing) AS frontage_missing_rows,
  COUNT(*) FILTER (WHERE bedrooms_missing) AS bedrooms_missing_rows,
  COUNT(*) FILTER (WHERE bathrooms_missing) AS bathrooms_missing_rows,
  COUNT(*) FILTER (WHERE floors_missing) AS floors_missing_rows,
  COUNT(*) FILTER (WHERE is_outlier_area) AS area_outlier_rows,
  COUNT(*) FILTER (WHERE is_outlier_price) AS price_outlier_rows,
  COUNT(*) FILTER (WHERE is_outlier_price_per_m2) AS price_per_m2_outlier_rows,
  COUNT(*) FILTER (WHERE is_robust_train_candidate) AS robust_train_candidate_rows
FROM warehouse.mart_avm_features_final
"""


def load_final_mart(engine) -> pd.DataFrame:
  with engine.connect() as conn:
    return pd.read_sql(text(FINAL_MART_QUERY), conn)


def load_summary_metrics(engine) -> dict[str, int]:
  with engine.connect() as conn:
    row = conn.execute(text(SUMMARY_QUERY)).mappings().one()
  return {key: int(value) for key, value in row.items()}
