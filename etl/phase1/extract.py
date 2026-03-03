#!/usr/bin/env python3
"""Raw extraction and staging dataframe preparation."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from .config import RAW_COLUMNS


def coerce_frontage(series: pd.Series) -> pd.Series:
    s = series.astype("string")
    s_clean = s.str.strip().str.lower()
    mapped = s_clean.map({"true": 1.0, "t": 1.0, "1": 1.0, "false": 0.0, "f": 0.0, "0": 0.0})
    numeric = pd.to_numeric(s, errors="coerce")
    return mapped.fillna(numeric)


def cast_numeric(df: pd.DataFrame, int_cols: Iterable[str], float_cols: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for col in int_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    for col in float_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def read_raw_text_dataframe(csv_path: str) -> pd.DataFrame:
    # Preserve source values as text for raw landing storage.
    return pd.read_csv(csv_path, usecols=RAW_COLUMNS, dtype="string")


def build_raw_landing_dataframe(df_raw_text: pd.DataFrame) -> pd.DataFrame:
    return df_raw_text.copy()[RAW_COLUMNS]


def read_raw_dataframe(csv_path: str) -> pd.DataFrame:
    # Backward-compatible alias used by existing callers.
    return read_raw_text_dataframe(csv_path)


def build_staging_dataframe(df_raw_source: pd.DataFrame) -> pd.DataFrame:
    df = df_raw_source.copy()

    df["id"] = df["id"].astype("string")
    df["detail_url"] = df["detail_url"].astype("string").str.strip()
    df["title"] = df["title"].astype("string").str.strip()
    df["location"] = df["location"].astype("string").str.strip()

    df = cast_numeric(
        df,
        int_cols=["timeline_hours", "bedrooms", "bathrooms", "floors"],
        float_cols=["area_m2", "price_million_vnd"],
    )
    df["frontage"] = coerce_frontage(df["frontage"])

    return df[RAW_COLUMNS]
