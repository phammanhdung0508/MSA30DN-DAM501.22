#!/usr/bin/env python3
"""Load and warehouse refresh helpers."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.types import Integer, Numeric, Text


def load_raw_landing(engine, raw_landing_df: pd.DataFrame) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE staging.stg_listings_raw_text;")

    raw_landing_df.to_sql(
        name="stg_listings_raw_text",
        schema="staging",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
        dtype={
            "id": Text(),
            "detail_url": Text(),
            "title": Text(),
            "location": Text(),
            "timeline_hours": Text(),
            "area_m2": Text(),
            "bedrooms": Text(),
            "bathrooms": Text(),
            "floors": Text(),
            "frontage": Text(),
            "price_million_vnd": Text(),
        },
    )


def load_staging(engine, staging_df: pd.DataFrame) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE staging.stg_listings_raw;")

    staging_df.to_sql(
        name="stg_listings_raw",
        schema="staging",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
        dtype={
            "id": Text(),
            "detail_url": Text(),
            "title": Text(),
            "location": Text(),
            "timeline_hours": Integer(),
            "area_m2": Numeric(),
            "bedrooms": Integer(),
            "bathrooms": Integer(),
            "floors": Integer(),
            "frontage": Numeric(),
            "price_million_vnd": Numeric(),
        },
    )


def load_fact(engine, fact_df: pd.DataFrame) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE warehouse.fact_listings;")

    fact_df.to_sql(
        name="fact_listings",
        schema="warehouse",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
        dtype={
            "listing_id": Text(),
            "detail_url": Text(),
            "title": Text(),
            "province": Text(),
            "district": Text(),
            "timeline_hours": Integer(),
            "area_m2": Numeric(),
            "bedrooms": Integer(),
            "bathrooms": Integer(),
            "floors": Integer(),
            "frontage": Numeric(),
            "price_million_vnd": Numeric(),
            "price_per_m2": Numeric(),
        },
    )


def refresh_location_dimension(engine) -> None:
    sql = """
    TRUNCATE TABLE warehouse.dim_location RESTART IDENTITY;

    INSERT INTO warehouse.dim_location (province, district)
    SELECT DISTINCT
      province,
      district
    FROM warehouse.fact_listings
    WHERE province IS NOT NULL OR district IS NOT NULL
    ORDER BY province, district;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def fetch_count(engine, qualified_table: str) -> int:
    with engine.connect() as conn:
        result = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {qualified_table};")
        return int(result.scalar_one())
