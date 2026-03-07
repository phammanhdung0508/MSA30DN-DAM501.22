"""CLI configuration for Phase 2 EDA."""

from __future__ import annotations

import argparse
import os


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="Phase 2 EDA for real-estate DW")
  parser.add_argument(
    "--db-uri",
    default=os.getenv("DW_PG_URI", ""),
    help="PostgreSQL connection URI. Fallback env: DW_PG_URI",
  )
  parser.add_argument(
    "--output-dir",
    default="reports/phase2",
    help="Output directory for EDA artifacts.",
  )
  parser.add_argument(
    "--min-district-listings",
    type=int,
    default=30,
    help="Minimum listing count for district ranking table.",
  )
  parser.add_argument(
    "--max-scatter-points",
    type=int,
    default=10000,
    help="Max sampled points for scatter plots.",
  )
  return parser.parse_args()
