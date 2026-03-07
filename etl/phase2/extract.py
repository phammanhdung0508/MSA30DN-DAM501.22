"""Data extraction from warehouse tables for Phase 2 EDA."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from .constants import FACT_QUERY, MART_QUERY, NUMERIC_COLUMNS


def load_data(db_uri: str) -> tuple[pd.DataFrame, pd.DataFrame]:
  engine = create_engine(db_uri, future=True)
  with engine.connect() as conn:
    fact = pd.read_sql(text(FACT_QUERY), conn)
    mart = pd.read_sql(text(MART_QUERY), conn)

  for col in NUMERIC_COLUMNS + ["timeline_hours"]:
    if col in fact.columns:
      fact[col] = pd.to_numeric(fact[col], errors="coerce")

  fact["province"] = fact["province"].fillna("Unknown").astype(str).str.strip()
  fact["district"] = fact["district"].fillna("Unknown").astype(str).str.strip()
  fact["detail_url"] = fact["detail_url"].fillna("").astype(str).str.strip()

  return fact, mart
