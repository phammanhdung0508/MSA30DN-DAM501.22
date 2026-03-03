#!/usr/bin/env python3
"""Load and warehouse refresh helpers."""

from __future__ import annotations

import uuid

import pandas as pd
from sqlalchemy.types import Integer, Numeric, Text


def _atomic_replace_dataframe(engine, schema: str, table: str, df: pd.DataFrame, dtype: dict[str, object]) -> None:
    tmp_table = f"__tmp_{table}_{uuid.uuid4().hex[:8]}"
    qualified_target = f"{schema}.{table}"
    qualified_tmp = f"{schema}.{tmp_table}"

    with engine.begin() as conn:
        df.to_sql(
            name=tmp_table,
            schema=schema,
            con=conn,
            if_exists="replace",
            index=False,
            chunksize=5000,
            method="multi",
            dtype=dtype,
        )
        conn.exec_driver_sql(f"TRUNCATE TABLE {qualified_target};")
        conn.exec_driver_sql(f"INSERT INTO {qualified_target} SELECT * FROM {qualified_tmp};")
        conn.exec_driver_sql(f"DROP TABLE {qualified_tmp};")


def load_raw_landing(engine, raw_landing_df: pd.DataFrame) -> None:
    _atomic_replace_dataframe(
        engine=engine,
        schema="staging",
        table="stg_listings_raw_text",
        df=raw_landing_df,
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
    _atomic_replace_dataframe(
        engine=engine,
        schema="staging",
        table="stg_listings_raw",
        df=staging_df,
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
    _atomic_replace_dataframe(
        engine=engine,
        schema="warehouse",
        table="fact_listings",
        df=fact_df,
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
