#!/usr/bin/env python3
"""Configuration and CLI parsing for Phase 1 ETL."""

from __future__ import annotations

import argparse
import os


RAW_COLUMNS = [
    "id",
    "detail_url",
    "title",
    "location",
    "timeline_hours",
    "area_m2",
    "bedrooms",
    "bathrooms",
    "floors",
    "frontage",
    "price_million_vnd",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 1 data preparation ETL.")
    parser.add_argument(
        "--db-uri",
        default=os.getenv("DW_PG_URI", ""),
        help="PostgreSQL connection URI. Fallback env: DW_PG_URI",
    )
    parser.add_argument(
        "--csv-path",
        default="data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv",
        help="Source CSV path.",
    )
    parser.add_argument(
        "--sql-dir",
        default="sql/phase1",
        help="Directory containing SQL scripts.",
    )
    parser.add_argument(
        "--skip-marts",
        action="store_true",
        help="Skip refreshing mart tables.",
    )
    parser.add_argument(
        "--skip-quality-gate",
        action="store_true",
        help="Skip pre-load quality gate checks.",
    )
    parser.add_argument(
        "--max-null-price-ratio",
        type=float,
        default=0.10,
        help="Maximum allowed null ratio for price_million_vnd in staging.",
    )
    parser.add_argument(
        "--max-invalid-price-ratio",
        type=float,
        default=0.02,
        help="Maximum allowed ratio for price_million_vnd <= 0 in staging.",
    )
    parser.add_argument(
        "--max-null-area-ratio",
        type=float,
        default=0.35,
        help="Maximum allowed null ratio for area_m2 in staging.",
    )
    parser.add_argument(
        "--max-invalid-area-ratio",
        type=float,
        default=0.01,
        help="Maximum allowed ratio for area_m2 <= 0 in staging.",
    )
    parser.add_argument(
        "--max-duplicate-detail-url-ratio",
        type=float,
        default=0.01,
        help="Maximum allowed duplicate ratio for non-empty detail_url in staging.",
    )
    parser.add_argument(
        "--min-fact-row-ratio",
        type=float,
        default=0.60,
        help="Minimum allowed ratio fact_rows/staging_rows after cleaning.",
    )
    return parser.parse_args()
