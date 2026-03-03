#!/usr/bin/env python3
"""Business transforms for warehouse fact construction."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Tuple

import pandas as pd

def remove_accents(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", text)


def normalize_spaces(value: str) -> str:
    value = re.sub(r"[\._\-/]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_province(token: Any) -> str | None:
    text = normalize_spaces(remove_accents(token).lower())
    if not text:
        return None

    text = re.sub(r"^(tp|thanh pho|tinh)\s+", "", text)
    province_aliases = {
        "hcm": "ho chi minh",
        "tp hcm": "ho chi minh",
        "tphcm": "ho chi minh",
        "ho chi minh city": "ho chi minh",
        "hn": "ha noi",
        "dn": "da nang",
    }
    text = province_aliases.get(text, text)
    return text.title()


def normalize_district(token: Any) -> str | None:
    original = normalize_spaces(remove_accents(token).lower())
    if not original:
        return None

    quan_match = re.match(r"^(quan|q)\s*(\d+)$", original)
    if quan_match:
        return f"Quan {quan_match.group(2)}"

    huyen_match = re.match(r"^(huyen|h)\s*(.+)$", original)
    if huyen_match:
        return f"Huyen {huyen_match.group(2).title()}"

    text = re.sub(r"^(quan|q|huyen|h|thi xa|tx|thanh pho|tp)\s+", "", original)
    text = normalize_spaces(text)
    return text.title() if text else None


def extract_location(location: Any) -> Tuple[str | None, str | None]:
    if location is None:
        return None, None

    raw = str(location).strip()
    if not raw:
        return None, None

    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        return None, None

    if len(parts) == 1:
        return normalize_province(parts[0]), None

    province_token = parts[-1]
    district_token = parts[-2]
    return normalize_province(province_token), normalize_district(district_token)


def build_fact_dataframe(staging_df: pd.DataFrame) -> pd.DataFrame:
    df = staging_df.copy()

    df = df[df["id"].notna()]
    df = df[df["price_million_vnd"] > 0]
    df = df[df["area_m2"] > 0]

    df["_detail_url_clean"] = df["detail_url"].fillna("").str.strip()
    df["_dedupe_key"] = df["_detail_url_clean"].where(
        df["_detail_url_clean"] != "", "id:" + df["id"].astype(str)
    )
    df["_timeline_sort"] = df["timeline_hours"].fillna(10**9)
    df["_completeness"] = df[["bedrooms", "bathrooms", "floors", "frontage"]].notna().sum(axis=1)

    df = df.sort_values(
        by=["_dedupe_key", "_timeline_sort", "_completeness", "id"],
        ascending=[True, True, False, True],
    )
    df = df.drop_duplicates(subset=["_dedupe_key"], keep="first")

    province_district = df["location"].map(extract_location)
    df["province"] = province_district.map(lambda value: value[0])
    df["district"] = province_district.map(lambda value: value[1])

    df["price_per_m2"] = df["price_million_vnd"] / df["area_m2"]

    fact = pd.DataFrame(
        {
            "listing_id": df["id"].astype(str),
            "detail_url": df["detail_url"],
            "title": df["title"],
            "province": df["province"],
            "district": df["district"],
            "timeline_hours": df["timeline_hours"].astype("Int64"),
            "area_m2": df["area_m2"],
            "bedrooms": df["bedrooms"].astype("Int64"),
            "bathrooms": df["bathrooms"].astype("Int64"),
            "floors": df["floors"].astype("Int64"),
            "frontage": df["frontage"],
            "price_million_vnd": df["price_million_vnd"],
            "price_per_m2": df["price_per_m2"],
        }
    )

    return fact.drop_duplicates(subset=["listing_id"], keep="first")
