"""CLI configuration for Phase 3 feature engineering."""

from __future__ import annotations

import argparse
import os


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="Phase 3 feature engineering for AVM")
  parser.add_argument(
    "--db-uri",
    default=os.getenv("DW_PG_URI", ""),
    help="PostgreSQL connection URI. Fallback env: DW_PG_URI",
  )
  parser.add_argument(
    "--sql-dir",
    default="sql/phase3",
    help="Directory containing Phase 3 SQL scripts.",
  )
  parser.add_argument(
    "--output-dir",
    default="reports/phase3",
    help="Output directory for Phase 3 artifacts.",
  )
  return parser.parse_args()
