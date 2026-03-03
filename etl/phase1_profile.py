#!/usr/bin/env python3
"""Phase 1 profiling for Vietnam real-estate buying listings."""

from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict

import pandas as pd


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
    parser = argparse.ArgumentParser(description="Profile raw listing CSV for Phase 1.")
    parser.add_argument(
        "--input-csv",
        default="data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv",
        help="Path to source CSV.",
    )
    parser.add_argument(
        "--output-json",
        default="reports/phase1_profiling_summary.json",
        help="Path to profiling output JSON.",
    )
    parser.add_argument(
        "--output-md",
        default="reports/phase1_profiling_summary.md",
        help="Path to profiling output Markdown.",
    )
    return parser.parse_args()


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


def normalize_location_token(value: Any) -> str:
    text = remove_accents(value).lower()
    text = re.sub(r"[\._\-/]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def stats_summary(series: pd.Series) -> Dict[str, float | int | None]:
    s = pd.to_numeric(series, errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna()
    if s.empty:
        return {
            "count": 0,
            "min": None,
            "p01": None,
            "p05": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p95": None,
            "p99": None,
            "max": None,
            "mean": None,
            "std": None,
        }

    q = s.quantile([0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).to_dict()
    return {
        "count": int(s.count()),
        "min": float(s.min()),
        "p01": float(q[0.01]),
        "p05": float(q[0.05]),
        "p25": float(q[0.25]),
        "p50": float(q[0.5]),
        "p75": float(q[0.75]),
        "p95": float(q[0.95]),
        "p99": float(q[0.99]),
        "max": float(s.max()),
        "mean": float(s.mean()),
        "std": float(s.std()),
    }


def iqr_outlier_summary(series: pd.Series) -> Dict[str, float | int | None]:
    s = pd.to_numeric(series, errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna()
    if s.empty:
        return {"count": 0, "ratio": 0.0, "lower": None, "upper": None}

    q1 = float(s.quantile(0.25))
    q3 = float(s.quantile(0.75))
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = ((s < lower) | (s > upper)).sum()
    return {
        "count": int(outliers),
        "ratio": float(outliers / len(s)),
        "lower": float(lower),
        "upper": float(upper),
    }


def analyze_location(df: pd.DataFrame) -> Dict[str, Any]:
    loc = df["location"].fillna("").astype(str).str.strip()
    non_empty = loc[loc != ""]

    split_tokens = non_empty.str.split(",")
    segment_counts = (non_empty.str.count(",") + 1).value_counts().sort_index()

    district_raw = split_tokens.map(lambda tokens: tokens[0].strip() if tokens else "")
    province_raw = split_tokens.map(lambda tokens: tokens[-1].strip() if tokens else "")

    province_norm = province_raw.map(normalize_location_token)
    district_norm = district_raw.map(normalize_location_token)

    return {
        "unique_location_raw": int(non_empty.nunique()),
        "top_location_raw": non_empty.value_counts().head(20).to_dict(),
        "segment_count_distribution": {int(k): int(v) for k, v in segment_counts.items()},
        "unique_province_raw": int(province_raw.nunique()),
        "unique_district_raw": int(district_raw.nunique()),
        "top_province_raw": province_raw.value_counts().head(20).to_dict(),
        "top_district_raw": district_raw.value_counts().head(20).to_dict(),
        "unique_province_normalized": int(province_norm.nunique()),
        "unique_district_normalized": int(district_norm.nunique()),
        "top_province_normalized": province_norm.value_counts().head(20).to_dict(),
    }


def build_report(df: pd.DataFrame, input_csv: str) -> Dict[str, Any]:
    row_count = int(len(df))
    price = pd.to_numeric(df["price_million_vnd"], errors="coerce")
    area = pd.to_numeric(df["area_m2"], errors="coerce")
    ppm2 = (price / area).replace([float("inf"), float("-inf")], pd.NA)

    missing_counts = df.isna().sum()
    missing = {
        col: {
            "count": int(missing_counts[col]),
            "ratio": float(missing_counts[col] / row_count) if row_count else math.nan,
        }
        for col in df.columns
    }

    detail_url = df["detail_url"].astype("string").str.strip()
    detail_url_all_key = detail_url.fillna("__MISSING__").replace("", "__MISSING__")
    non_empty_url = detail_url[detail_url.notna() & (detail_url != "")]

    duplicate_id_mask = df["id"].duplicated(keep=False)
    duplicate_url_mask_all = detail_url.duplicated(keep=False)
    duplicate_url_mask = non_empty_url.duplicated(keep=False)

    duplicate = {
        "duplicate_id_rows": int(duplicate_id_mask.sum()),
        "duplicate_id_keys": int(df.loc[df["id"].duplicated(), "id"].nunique()),
        "duplicate_detail_url_rows_all": int(duplicate_url_mask_all.sum()),
        "duplicate_detail_url_keys_all": int(detail_url_all_key[detail_url_all_key.duplicated()].nunique()),
        "duplicate_detail_url_rows": int(duplicate_url_mask.sum()),
        "duplicate_detail_url_keys": int(non_empty_url[non_empty_url.duplicated()].nunique()),
        "full_row_duplicates": int(df.duplicated().sum()),
    }

    report = {
        "input_csv": input_csv,
        "row_count": row_count,
        "column_count": int(df.shape[1]),
        "columns": list(df.columns),
        "missing": missing,
        "duplicate": duplicate,
        "invalid": {
            "price_non_positive_count": int((price <= 0).sum()),
            "area_non_positive_count": int((area <= 0).sum()),
            "price_per_m2_non_positive_count": int((ppm2 <= 0).fillna(False).sum()),
        },
        "distribution": {
            "price_million_vnd": stats_summary(price),
            "area_m2": stats_summary(area),
            "price_per_m2": stats_summary(ppm2),
        },
        "outlier_iqr": {
            "price_million_vnd": iqr_outlier_summary(price),
            "area_m2": iqr_outlier_summary(area),
            "price_per_m2": iqr_outlier_summary(ppm2),
        },
        "location_analysis": analyze_location(df),
    }
    return report


def fmt_pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def markdown_report(report: Dict[str, Any]) -> str:
    missing = report["missing"]
    duplicate = report["duplicate"]
    invalid = report["invalid"]
    dist = report["distribution"]
    outliers = report["outlier_iqr"]
    loc = report["location_analysis"]

    lines = []
    lines.append("# Phase 1 Data Profiling Summary")
    lines.append("")
    lines.append(f"- Input CSV: `{report['input_csv']}`")
    lines.append(f"- Row count: **{report['row_count']:,}**")
    lines.append(f"- Column count: **{report['column_count']}**")
    lines.append("")

    lines.append("## Missing Values")
    lines.append("")
    lines.append("| column | missing_count | missing_ratio |")
    lines.append("|---|---:|---:|")
    for col, metric in missing.items():
        lines.append(f"| {col} | {metric['count']:,} | {fmt_pct(metric['ratio'])} |")
    lines.append("")

    lines.append("## Duplicates")
    lines.append("")
    lines.append(f"- Duplicate rows by `id` (all repeated rows): **{duplicate['duplicate_id_rows']:,}**")
    lines.append(f"- Duplicate keys by `id`: **{duplicate['duplicate_id_keys']:,}**")
    lines.append(
        f"- Duplicate rows by `detail_url` (including empty/null): **{duplicate['duplicate_detail_url_rows_all']:,}**"
    )
    lines.append(
        f"- Duplicate keys by `detail_url` (including empty/null): **{duplicate['duplicate_detail_url_keys_all']:,}**"
    )
    lines.append(
        f"- Duplicate rows by non-empty `detail_url` (all repeated rows): **{duplicate['duplicate_detail_url_rows']:,}**"
    )
    lines.append(f"- Duplicate keys by non-empty `detail_url`: **{duplicate['duplicate_detail_url_keys']:,}**")
    lines.append(f"- Exact full-row duplicates: **{duplicate['full_row_duplicates']:,}**")
    lines.append("")

    lines.append("## Invalid Value Checks")
    lines.append("")
    lines.append(f"- `price_million_vnd <= 0`: **{invalid['price_non_positive_count']:,}**")
    lines.append(f"- `area_m2 <= 0`: **{invalid['area_non_positive_count']:,}**")
    lines.append(f"- `price_per_m2 <= 0` (computed): **{invalid['price_per_m2_non_positive_count']:,}**")
    lines.append("")

    lines.append("## Distribution Summary")
    lines.append("")
    for metric_name, metric in dist.items():
        lines.append(f"### {metric_name}")
        lines.append("")
        lines.append(
            "- "
            + ", ".join(
                [
                    f"count={metric['count']:,}",
                    f"min={metric['min']}",
                    f"p50={metric['p50']}",
                    f"p95={metric['p95']}",
                    f"p99={metric['p99']}",
                    f"max={metric['max']}",
                    f"mean={metric['mean']}",
                ]
            )
        )
        out = outliers[metric_name]
        lines.append(
            f"- IQR outliers: {out['count']:,} rows ({fmt_pct(out['ratio'])}), bounds=({out['lower']}, {out['upper']})"
        )
        lines.append("")

    lines.append("## Location Format Analysis")
    lines.append("")
    lines.append(f"- Unique raw location strings: **{loc['unique_location_raw']:,}**")
    lines.append(f"- Unique raw provinces: **{loc['unique_province_raw']:,}**")
    lines.append(f"- Unique raw districts: **{loc['unique_district_raw']:,}**")
    lines.append(f"- Unique normalized provinces (accent removed): **{loc['unique_province_normalized']:,}**")
    lines.append(f"- Unique normalized districts (accent removed): **{loc['unique_district_normalized']:,}**")
    lines.append("")
    lines.append("### Segment Distribution")
    lines.append("")
    for seg, cnt in loc["segment_count_distribution"].items():
        lines.append(f"- {seg} segment(s): {cnt:,}")
    lines.append("")

    lines.append("### Top 10 Raw Locations")
    lines.append("")
    for idx, (name, cnt) in enumerate(list(loc["top_location_raw"].items())[:10], start=1):
        lines.append(f"{idx}. {name}: {cnt:,}")
    lines.append("")

    lines.append("## Recommended Cleaning Rules")
    lines.append("")
    lines.append("- Remove rows where `price_million_vnd <= 0`.")
    lines.append("- Remove rows where `area_m2 <= 0`.")
    lines.append("- Compute `price_per_m2 = price_million_vnd / area_m2`.")
    lines.append("- De-duplicate by non-empty `detail_url`; fallback key `id` when URL missing.")
    lines.append("- Standardize location by splitting `district, province`, removing accents, and normalizing admin prefixes.")
    lines.append("- Keep `timeline_hours` as feature and derive time buckets for marts.")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    input_csv = Path(args.input_csv)
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)

    df = pd.read_csv(input_csv, usecols=RAW_COLUMNS)
    report = build_report(df, str(input_csv))

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)

    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    output_md.write_text(markdown_report(report), encoding="utf-8")

    print(f"Profiling complete: {len(df):,} rows")
    print(f"JSON report: {output_json}")
    print(f"Markdown report: {output_md}")


if __name__ == "__main__":
    main()
