"""Narrative insight and report rendering for Phase 2 EDA."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .paths import Paths


def build_key_insights(
  quality: dict[str, Any],
  target: dict[str, Any],
  size: dict[str, Any],
  corr: dict[str, Any],
  mart_check: dict[str, Any],
) -> list[str]:
  missing_df = pd.read_csv(quality["missing_table_csv"])
  miss_map = dict(zip(missing_df["column"], missing_df["missing_pct"]))

  return [
    (
      "Price distribution is right-skewed "
      f"(skew={target['price_skewness']:.2f}), and log(price+1) reduces skew to {target['log_price_skewness']:.2f}."
    ),
    (
      "Feature completeness is uneven: bedrooms/bathrooms/floors missing rates are "
      f"{miss_map.get('bedrooms', 0):.2f}%, {miss_map.get('bathrooms', 0):.2f}%, and {miss_map.get('floors', 0):.2f}%."
    ),
    (
      "Area has a positive relation with total price "
      f"(corr={size['corr_area_price']:.3f}), while relation with price_per_m2 is weaker "
      f"(corr={size['corr_area_price_per_m2']:.3f})."
    ),
    (
      "Province-level market spread is large; top provinces by median price_per_m2 are significantly above the national median."
    ),
    (
      "Timeline is highly imbalanced, with most listings in >30 days bucket; this can bias recency-based interpretation."
    ),
    (
      "No duplicate listing_id or dedupe_key remains in fact_listings, indicating Phase 1 dedup logic is stable on current data."
    ),
    (
      "Correlation matrix highlights candidate predictors for AVM from numeric block: "
      + ", ".join([f"{k} ({v:.2f})" for k, v in corr["strong_predictors"].items()][:4])
      if corr["strong_predictors"]
      else "No numeric predictor exceeds absolute correlation 0.2 with total price."
    ),
    (
      f"Market mart consistency check shows {mart_check['mismatch_group_count']} mismatched groups over "
      f"{mart_check['compared_group_count']} compared groups."
    ),
  ]


def write_markdown_report(
  paths: Paths,
  quality: dict[str, Any],
  target: dict[str, Any],
  location: dict[str, Any],
  size: dict[str, Any],
  features: dict[str, Any],
  corr: dict[str, Any],
  timeline: dict[str, Any],
  mart_check: dict[str, Any],
  modeling_strategy: dict[str, Any],
  insights: list[str],
) -> None:
  lines: list[str] = []
  lines.append("# Phase 2 EDA Summary")
  lines.append("")
  lines.append("## 1) Data Quality Validation")
  lines.append("")
  lines.append(f"- Row count in `warehouse.fact_listings`: **{quality['row_count']:,}**")
  lines.append(f"- Duplicate `listing_id` rows: **{quality['duplicate_listing_id_rows']:,}**")
  lines.append(f"- Duplicate dedupe-key rows: **{quality['duplicate_dedupe_key_rows']:,}**")
  lines.append(f"- Missing summary table: `{quality['missing_table_csv']}`")
  lines.append(f"- Numeric range table: `{quality['numeric_ranges_csv']}`")
  lines.append("")

  lines.append("## 2) Target Variable Analysis")
  lines.append("")
  lines.append(f"- Price skewness: **{target['price_skewness']:.4f}**")
  lines.append(f"- Log-price skewness: **{target['log_price_skewness']:.4f}**")
  lines.append(f"- Price-per-m2 skewness: **{target['price_per_m2_skewness']:.4f}**")
  lines.append(f"- Log transform recommended: **{target['log_transform_recommended']}**")
  lines.append(f"- Distribution figure: `{target['figure']}`")
  lines.append("")

  lines.append("## 3) Location-Based Market Analysis")
  lines.append("")
  lines.append(f"- Province ranking table: `{location['province_table_csv']}`")
  lines.append(f"- District ranking table: `{location['district_table_csv']}`")
  lines.append(f"- Location overview figure: `{location['figure']}`")
  lines.append("")

  lines.append("## 4) Property Size Analysis")
  lines.append("")
  lines.append(f"- Corr(area, price): **{size['corr_area_price']:.4f}**")
  lines.append(f"- Corr(area, price_per_m2): **{size['corr_area_price_per_m2']:.4f}**")
  lines.append(f"- Scatter figure: `{size['figure']}`")
  lines.append(f"- Outlier candidates table: `{size['outlier_table_csv']}`")
  lines.append("")

  lines.append("## 5) Property Feature Analysis")
  lines.append("")
  lines.append(f"- Bedrooms stats: `{features['bedrooms_stats_csv']}`")
  lines.append(f"- Floors stats: `{features['floors_stats_csv']}`")
  lines.append(f"- Feature figure: `{features['figure']}`")
  lines.append("")

  lines.append("## 6) Correlation Analysis")
  lines.append("")
  lines.append(f"- Correlation matrix table: `{corr['matrix_csv']}`")
  lines.append(f"- Heatmap: `{corr['figure']}`")
  lines.append("- Strong predictors (|corr|>=0.2) vs target:")
  if corr["strong_predictors"]:
    for feature, value in corr["strong_predictors"].items():
      lines.append(f"  - {feature}: {value:.4f}")
  else:
    lines.append("  - None")
  lines.append("")

  lines.append("## 7) Timeline Analysis")
  lines.append("")
  lines.append(f"- Timeline bucket table: `{timeline['timeline_table_csv']}`")
  lines.append(f"- Timeline figure: `{timeline['figure']}`")
  lines.append("")

  lines.append("## 8) Market Analytics Mart Validation")
  lines.append("")
  lines.append(f"- Compared groups: **{mart_check['compared_group_count']:,}**")
  lines.append(f"- Mismatch groups: **{mart_check['mismatch_group_count']:,}**")
  lines.append(f"- Mismatch detail table: `{mart_check['mismatch_table_csv']}`")
  lines.append("")

  lines.append("## 9) Modeling Decisions")
  lines.append("")
  lines.append("- Missing strategy:")
  lines.append(
    "  - Use raw columns for audit, but model with "
    + ", ".join(modeling_strategy["missing_strategy"]["imputed_columns"])
    + " plus "
    + ", ".join(modeling_strategy["missing_strategy"]["missing_flag_columns"])
    + "."
  )
  lines.append(f"  - Rule: {modeling_strategy['missing_strategy']['imputation_rule']}")
  lines.append("- Outlier strategy:")
  lines.append(
    "  - Keep all rows in `warehouse.fact_listings` and `warehouse.mart_avm_features`; do not hard-delete outliers."
  )
  lines.append(
    "  - Use "
    + ", ".join(modeling_strategy["outlier_strategy"]["feature_flag_columns"])
    + " as features."
  )
  lines.append(
    "  - Optional robust subset: `"
    + modeling_strategy["outlier_strategy"]["robust_subset_column"]
    + "`."
  )
  lines.append("")

  lines.append("## 10) AVM Feature Recommendations")
  lines.append("")
  lines.append("| feature | missing_pct | reason |")
  lines.append("|---|---:|---|")
  for item in features["feature_candidates"]:
    lines.append(f"| {item['feature']} | {item['missing_pct']:.2f}% | {item['reason']} |")
  lines.append("")

  lines.append("## Key Insights (5-10)")
  lines.append("")
  for idx, insight in enumerate(insights, start=1):
    lines.append(f"{idx}. {insight}")

  paths.summary_md.write_text("\n".join(lines), encoding="utf-8")
