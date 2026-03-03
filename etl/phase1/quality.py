#!/usr/bin/env python3
"""Data quality gate checks for Phase 1 ETL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class QualityThresholds:
    max_null_price_ratio: float
    max_invalid_price_ratio: float
    max_null_area_ratio: float
    max_invalid_area_ratio: float
    max_duplicate_detail_url_ratio: float
    min_fact_row_ratio: float


class QualityGateError(ValueError):
    """Raised when data quality gate thresholds are violated."""



def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator / denominator)


def compute_quality_metrics(staging_df: pd.DataFrame, fact_df: pd.DataFrame) -> dict[str, Any]:
    total_rows = int(len(staging_df))

    price = pd.to_numeric(staging_df["price_million_vnd"], errors="coerce")
    area = pd.to_numeric(staging_df["area_m2"], errors="coerce")

    null_price_count = int(price.isna().sum())
    invalid_price_count = int((price <= 0).fillna(False).sum())
    null_area_count = int(area.isna().sum())
    invalid_area_count = int((area <= 0).fillna(False).sum())

    detail_url = staging_df["detail_url"].astype("string").str.strip()
    non_empty_url = detail_url[detail_url.notna() & (detail_url != "")]
    duplicate_non_empty_url_rows = int(non_empty_url.duplicated(keep=False).sum())

    fact_rows = int(len(fact_df))

    metrics = {
        "total_rows": total_rows,
        "fact_rows": fact_rows,
        "null_price_count": null_price_count,
        "invalid_price_count": invalid_price_count,
        "null_area_count": null_area_count,
        "invalid_area_count": invalid_area_count,
        "duplicate_non_empty_detail_url_rows": duplicate_non_empty_url_rows,
        "null_price_ratio": _safe_ratio(null_price_count, total_rows),
        "invalid_price_ratio": _safe_ratio(invalid_price_count, total_rows),
        "null_area_ratio": _safe_ratio(null_area_count, total_rows),
        "invalid_area_ratio": _safe_ratio(invalid_area_count, total_rows),
        "duplicate_non_empty_detail_url_ratio": _safe_ratio(duplicate_non_empty_url_rows, total_rows),
        "fact_row_ratio": _safe_ratio(fact_rows, total_rows),
    }
    return metrics


def enforce_quality_gate(metrics: dict[str, Any], thresholds: QualityThresholds) -> None:
    violations: list[str] = []

    if metrics["null_price_ratio"] > thresholds.max_null_price_ratio:
        violations.append(
            f"null_price_ratio={metrics['null_price_ratio']:.4f} > {thresholds.max_null_price_ratio:.4f}"
        )
    if metrics["invalid_price_ratio"] > thresholds.max_invalid_price_ratio:
        violations.append(
            f"invalid_price_ratio={metrics['invalid_price_ratio']:.4f} > {thresholds.max_invalid_price_ratio:.4f}"
        )
    if metrics["null_area_ratio"] > thresholds.max_null_area_ratio:
        violations.append(
            f"null_area_ratio={metrics['null_area_ratio']:.4f} > {thresholds.max_null_area_ratio:.4f}"
        )
    if metrics["invalid_area_ratio"] > thresholds.max_invalid_area_ratio:
        violations.append(
            f"invalid_area_ratio={metrics['invalid_area_ratio']:.4f} > {thresholds.max_invalid_area_ratio:.4f}"
        )
    if metrics["duplicate_non_empty_detail_url_ratio"] > thresholds.max_duplicate_detail_url_ratio:
        violations.append(
            "duplicate_non_empty_detail_url_ratio="
            f"{metrics['duplicate_non_empty_detail_url_ratio']:.4f} > {thresholds.max_duplicate_detail_url_ratio:.4f}"
        )
    if metrics["fact_row_ratio"] < thresholds.min_fact_row_ratio:
        violations.append(
            f"fact_row_ratio={metrics['fact_row_ratio']:.4f} < {thresholds.min_fact_row_ratio:.4f}"
        )

    if violations:
        raise QualityGateError("Quality gate failed: " + "; ".join(violations))
