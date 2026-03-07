"""Reporting helpers for Phase 3 feature engineering."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .constants import (
  FINAL_EXCLUDED_FEATURES,
  FINAL_OPTIONAL_FEATURES,
  FINAL_REQUIRED_FEATURES,
  INPUT_FEATURE_REVIEW,
  LEAKAGE_GUIDANCE,
  LOCATION_ENCODING_GUIDANCE,
  NUMERIC_TRANSFORMS,
  TARGET_DECISION,
  TIME_FEATURES,
)
from .paths import Paths, save_table


def build_feature_review_tables(paths: Paths) -> dict[str, str]:
  input_review = pd.DataFrame(INPUT_FEATURE_REVIEW)
  numeric_transforms = pd.DataFrame(NUMERIC_TRANSFORMS)
  time_features = pd.DataFrame(TIME_FEATURES)
  selection = pd.DataFrame(
    [
      {"feature": feature, "selection": "required"}
      for feature in FINAL_REQUIRED_FEATURES
    ]
    + [
      {"feature": feature, "selection": "optional"}
      for feature in FINAL_OPTIONAL_FEATURES
    ]
    + [
      {"feature": feature, "selection": "exclude_from_predictors"}
      for feature in FINAL_EXCLUDED_FEATURES
    ]
  )

  save_table(input_review, paths.tables / "input_feature_review.csv")
  save_table(numeric_transforms, paths.tables / "numeric_transform_decisions.csv")
  save_table(time_features, paths.tables / "time_feature_decisions.csv")
  save_table(selection, paths.tables / "final_feature_selection.csv")

  return {
    "input_feature_review_csv": str(paths.tables / "input_feature_review.csv"),
    "numeric_transform_decisions_csv": str(paths.tables / "numeric_transform_decisions.csv"),
    "time_feature_decisions_csv": str(paths.tables / "time_feature_decisions.csv"),
    "final_feature_selection_csv": str(paths.tables / "final_feature_selection.csv"),
  }


def write_summary(
  paths: Paths,
  metrics: dict[str, Any],
  artifacts: dict[str, str],
) -> None:
  payload = {
    "metrics": metrics,
    "target_decision": TARGET_DECISION,
    "location_encoding_guidance": LOCATION_ENCODING_GUIDANCE,
    "leakage_guidance": LEAKAGE_GUIDANCE,
    "required_features": FINAL_REQUIRED_FEATURES,
    "optional_features": FINAL_OPTIONAL_FEATURES,
    "excluded_features": FINAL_EXCLUDED_FEATURES,
    "artifacts": artifacts,
  }
  paths.summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

  lines = [
    "# Phase 3 Feature Engineering Summary",
    "",
    "## 1) Final Output Dataset",
    "",
    "- Table: `warehouse.mart_avm_features_final`",
    f"- Row count: **{metrics['row_count']:,}**",
    f"- Robust-train candidate rows: **{metrics['robust_train_candidate_rows']:,}**",
    f"- Area-outlier rows: **{metrics['area_outlier_rows']:,}**",
    "",
    "## 2) Reused Decisions From Phase 2",
    "",
    "- Bedrooms, bathrooms, and floors reuse the Phase 2 location-aware median imputation strategy.",
    "- Outlier flags are reused, but target-derived flags are now explicitly marked as analysis/filtering only.",
    "- `frontage` is reinterpreted as a boolean presence signal because the raw CSV stores it as true/false.",
    "",
    "## 3) Target Transformation",
    "",
    f"- Model target: `{TARGET_DECISION['transformed_target']}`",
    f"- Raw target retained: `{TARGET_DECISION['raw_target']}`",
    f"- Rationale: {TARGET_DECISION['reason']}",
    f"- Recommendation: {TARGET_DECISION['recommendation']}",
    "",
    "## 4) Numeric Feature Strategy",
    "",
  ]
  for item in NUMERIC_TRANSFORMS:
    lines.append(f"- `{item['feature']}`: {item['decision']} - {item['why']}")

  lines.extend(
    [
      "",
      "## 5) Time Features",
      "",
    ]
  )
  for item in TIME_FEATURES:
    lines.append(f"- `{item['feature']}` for {item['use_for']}: {item['why']}")

  lines.extend(
    [
      "",
      "## 6) Location Encoding Guidance",
      "",
      f"- Tree models: {LOCATION_ENCODING_GUIDANCE['tree_models']['recommended']}",
      f"- CatBoost: {LOCATION_ENCODING_GUIDANCE['tree_models']['catboost']}",
      f"- LightGBM: {LOCATION_ENCODING_GUIDANCE['tree_models']['lightgbm']}",
      f"- Linear models: {LOCATION_ENCODING_GUIDANCE['linear_models']['recommended']}",
      "",
      "## 7) Leakage-safe Market Aggregate Strategy",
      "",
      f"- Market aggregates: {LEAKAGE_GUIDANCE['market_aggregates']}",
      f"- OOF training features: {LEAKAGE_GUIDANCE['oof_training_features']}",
      f"- Target-derived outlier flags: {LEAKAGE_GUIDANCE['target_outlier_flags']}",
      f"- Warehouse table semantics: {LEAKAGE_GUIDANCE['warehouse_table']}",
      "",
      "## 8) Final Feature Selection",
      "",
      "### Required features",
      "",
    ]
  )
  for feature in FINAL_REQUIRED_FEATURES:
    lines.append(f"- `{feature}`")

  lines.extend(["", "### Optional features", ""])
  for feature in FINAL_OPTIONAL_FEATURES:
    lines.append(f"- `{feature}`")

  lines.extend(["", "### Excluded from predictors", ""])
  for feature in FINAL_EXCLUDED_FEATURES:
    lines.append(f"- `{feature}`")

  lines.extend(
    [
      "",
      "## 9) Generated Artifacts",
      "",
      f"- Input feature review: `{artifacts['input_feature_review_csv']}`",
      f"- Numeric transform decisions: `{artifacts['numeric_transform_decisions_csv']}`",
      f"- Time feature decisions: `{artifacts['time_feature_decisions_csv']}`",
      f"- Final feature selection: `{artifacts['final_feature_selection_csv']}`",
      "",
      "## 10) Validation Snapshot",
      "",
      f"- `timeline_missing_rows`: **{metrics['timeline_missing_rows']:,}**",
      f"- `frontage_missing_rows`: **{metrics['frontage_missing_rows']:,}**",
      f"- `bedrooms_missing_rows`: **{metrics['bedrooms_missing_rows']:,}**",
      f"- `bathrooms_missing_rows`: **{metrics['bathrooms_missing_rows']:,}**",
      f"- `floors_missing_rows`: **{metrics['floors_missing_rows']:,}**",
      f"- `price_outlier_rows`: **{metrics['price_outlier_rows']:,}**",
      f"- `price_per_m2_outlier_rows`: **{metrics['price_per_m2_outlier_rows']:,}**",
      "",
      "## 11) Best Practices For Phase 4",
      "",
      "- Split train/validation before fitting frequency or market aggregate encoders.",
      "- Train on `target_log_price_million_vnd`, then invert with `expm1` for business reporting.",
      "- For CatBoost, keep raw categoricals and avoid manual one-hot encoding for `district`.",
      "- For linear baselines, use one-hot province/district/timeline_bucket and scale continuous variables outside the warehouse.",
    ]
  )

  paths.summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
