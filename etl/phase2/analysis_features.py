"""Size/feature correlation analysis and AVM feature recommendation for Phase 2 EDA."""

from __future__ import annotations

from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .paths import Paths, save_table


def analyze_property_size(
  fact: pd.DataFrame,
  paths: Paths,
  max_scatter_points: int,
) -> dict[str, Any]:
  sample = fact
  if len(sample) > max_scatter_points:
    sample = sample.sample(n=max_scatter_points, random_state=42)

  corr_area_price = float(fact[["area_m2", "price_million_vnd"]].corr().iloc[0, 1])
  corr_area_ppm2 = float(fact[["area_m2", "price_per_m2"]].corr().iloc[0, 1])

  area_hi = float(fact["area_m2"].quantile(0.995))
  ppm2_hi = float(fact["price_per_m2"].quantile(0.995))
  outlier_candidates = fact[(fact["area_m2"] >= area_hi) | (fact["price_per_m2"] >= ppm2_hi)].copy()
  outlier_cols = ["listing_id", "province", "district", "area_m2", "price_million_vnd", "price_per_m2"]
  outlier_candidates = outlier_candidates[outlier_cols].sort_values(["area_m2", "price_per_m2"], ascending=False)
  save_table(outlier_candidates.head(200), paths.tables / "size_outlier_candidates.csv")

  fig, axes = plt.subplots(1, 2, figsize=(18, 6))
  sns.scatterplot(data=sample, x="area_m2", y="price_million_vnd", alpha=0.25, s=16, ax=axes[0], color="#2C7FB8")
  axes[0].set_title("Area vs Total Price")
  axes[0].set_xlim(left=0)
  axes[0].set_ylim(bottom=0)

  sns.scatterplot(data=sample, x="area_m2", y="price_per_m2", alpha=0.25, s=16, ax=axes[1], color="#F03B20")
  axes[1].set_title("Area vs Price per m2")
  axes[1].set_xlim(left=0)
  axes[1].set_ylim(bottom=0)

  fig.tight_layout()
  out_path = paths.figures / "size_relationship_scatter.png"
  fig.savefig(out_path, dpi=180)
  plt.close(fig)

  return {
    "corr_area_price": corr_area_price,
    "corr_area_price_per_m2": corr_area_ppm2,
    "outlier_table_csv": str(paths.tables / "size_outlier_candidates.csv"),
    "figure": str(out_path),
  }


def analyze_property_features(fact: pd.DataFrame, paths: Paths) -> dict[str, Any]:
  bedrooms_stats = (
    fact.dropna(subset=["bedrooms"])
    .query("bedrooms >= 0 and bedrooms <= 10")
    .groupby("bedrooms", as_index=False)
    .agg(
      listing_count=("listing_id", "count"),
      median_price_million_vnd=("price_million_vnd", "median"),
      median_price_per_m2=("price_per_m2", "median"),
    )
    .sort_values("bedrooms")
  )
  floors_stats = (
    fact.dropna(subset=["floors"])
    .query("floors >= 0 and floors <= 15")
    .groupby("floors", as_index=False)
    .agg(
      listing_count=("listing_id", "count"),
      median_price_million_vnd=("price_million_vnd", "median"),
      median_price_per_m2=("price_per_m2", "median"),
    )
    .sort_values("floors")
  )

  save_table(bedrooms_stats, paths.tables / "feature_bedrooms_stats.csv")
  save_table(floors_stats, paths.tables / "feature_floors_stats.csv")

  plot_df = fact.copy()
  plot_df = plot_df[(plot_df["bedrooms"].between(0, 10, inclusive="both")) | (plot_df["bedrooms"].isna())]
  plot_df = plot_df[(plot_df["floors"].between(0, 15, inclusive="both")) | (plot_df["floors"].isna())]

  fig, axes = plt.subplots(2, 2, figsize=(16, 12))
  sns.boxplot(data=plot_df.dropna(subset=["bedrooms"]), x="bedrooms", y="price_million_vnd", ax=axes[0, 0], color="#74A9CF")
  axes[0, 0].set_title("Price vs Bedrooms")
  axes[0, 0].set_ylim(0, plot_df["price_million_vnd"].quantile(0.99))

  sns.boxplot(data=plot_df.dropna(subset=["floors"]), x="floors", y="price_million_vnd", ax=axes[0, 1], color="#FD8D3C")
  axes[0, 1].set_title("Price vs Floors")
  axes[0, 1].set_ylim(0, plot_df["price_million_vnd"].quantile(0.99))

  sns.histplot(plot_df["frontage"], bins=50, ax=axes[1, 0], color="#31A354")
  axes[1, 0].set_title("Frontage Distribution")

  sns.histplot(plot_df["bathrooms"].dropna(), bins=20, ax=axes[1, 1], color="#756BB1")
  axes[1, 1].set_title("Bathrooms Distribution")

  fig.tight_layout()
  out_path = paths.figures / "property_feature_analysis.png"
  fig.savefig(out_path, dpi=180)
  plt.close(fig)

  corr_features = {
    "bedrooms": float(fact[["bedrooms", "price_million_vnd"]].corr().iloc[0, 1]),
    "bathrooms": float(fact[["bathrooms", "price_million_vnd"]].corr().iloc[0, 1]),
    "floors": float(fact[["floors", "price_million_vnd"]].corr().iloc[0, 1]),
    "frontage": float(fact[["frontage", "price_million_vnd"]].corr().iloc[0, 1]),
  }

  return {
    "bedrooms_stats_csv": str(paths.tables / "feature_bedrooms_stats.csv"),
    "floors_stats_csv": str(paths.tables / "feature_floors_stats.csv"),
    "corr_with_price": corr_features,
    "figure": str(out_path),
  }


def analyze_correlations(fact: pd.DataFrame, paths: Paths) -> dict[str, Any]:
  corr_cols = ["area_m2", "bedrooms", "bathrooms", "floors", "frontage", "price_million_vnd", "price_per_m2"]
  corr_df = fact[corr_cols].corr(numeric_only=True)
  save_table(corr_df.reset_index().rename(columns={"index": "variable"}), paths.tables / "correlation_matrix.csv")

  fig, ax = plt.subplots(figsize=(9, 7))
  sns.heatmap(corr_df, annot=True, fmt=".2f", cmap="YlGnBu", square=True, ax=ax)
  ax.set_title("Correlation Matrix (Numeric Features)")
  fig.tight_layout()
  out_path = paths.figures / "correlation_heatmap.png"
  fig.savefig(out_path, dpi=180)
  plt.close(fig)

  target_corr = corr_df["price_million_vnd"].drop(labels=["price_million_vnd"]).sort_values(key=np.abs, ascending=False)
  strong_predictors = target_corr[abs(target_corr) >= 0.2].to_dict()

  feature_corr = corr_df.drop(index=["price_million_vnd", "price_per_m2"], columns=["price_million_vnd", "price_per_m2"])
  multicollinear_pairs: list[dict[str, Any]] = []
  cols = list(feature_corr.columns)
  for i, col_i in enumerate(cols):
    for col_j in cols[i + 1 :]:
      value = float(feature_corr.loc[col_i, col_j])
      if abs(value) >= 0.7:
        multicollinear_pairs.append({"feature_1": col_i, "feature_2": col_j, "corr": value})

  return {
    "matrix_csv": str(paths.tables / "correlation_matrix.csv"),
    "figure": str(out_path),
    "strong_predictors": strong_predictors,
    "multicollinear_pairs": multicollinear_pairs,
  }


def recommend_avm_features(quality: dict[str, Any], correlations: dict[str, Any]) -> dict[str, Any]:
  missing_df = pd.read_csv(quality["missing_table_csv"])
  missing_map = dict(zip(missing_df["column"], missing_df["missing_pct"]))

  candidates = [
    ("area_m2", "Strong structural size signal for total price."),
    ("bedrooms_imputed", "Location-aware median-imputed bedroom count for modeling."),
    ("bathrooms_imputed", "Location-aware median-imputed bathroom count for modeling."),
    ("floors_imputed", "Location-aware median-imputed floor count for modeling."),
    ("bedrooms_missing", "Preserves the information that bedroom count was not disclosed."),
    ("bathrooms_missing", "Preserves the information that bathroom count was not disclosed."),
    ("floors_missing", "Preserves the information that floor count was not disclosed."),
    ("frontage", "Commercial exposure and access proxy."),
    ("province", "Macro regional price level."),
    ("district", "Micro location premium."),
    ("timeline_hours", "Listing freshness and pricing behavior."),
    ("district_median_price_million_vnd", "Contextual market baseline by district."),
    ("district_median_price_per_m2", "Location-normalized pricing baseline."),
    ("is_outlier_price", "Flags atypical total-price observations for robust modeling decisions."),
    ("is_outlier_area", "Flags atypical property size observations."),
    ("is_outlier_price_per_m2", "Flags atypical unit-price observations."),
    ("is_outlier_any", "Compact indicator that at least one outlier rule fired."),
  ]

  output = []
  for feature, reason in candidates:
    output.append(
      {
        "feature": feature,
        "missing_pct": float(missing_map.get(feature, 0.0)),
        "reason": reason,
      }
    )

  return {
    "feature_candidates": output,
    "strong_numeric_predictors": correlations["strong_predictors"],
  }


def build_modeling_strategy(quality: dict[str, Any]) -> dict[str, Any]:
  missing_df = pd.read_csv(quality["missing_table_csv"])
  missing_map = dict(zip(missing_df["column"], missing_df["missing_pct"]))

  return {
    "missing_strategy": {
      "decision": "retain_raw_and_use_imputed_plus_missing_flags",
      "target_columns": ["bedrooms", "bathrooms", "floors"],
      "imputed_columns": ["bedrooms_imputed", "bathrooms_imputed", "floors_imputed"],
      "missing_flag_columns": ["bedrooms_missing", "bathrooms_missing", "floors_missing"],
      "imputation_rule": (
        "Use district median when at least 30 observed values exist for the feature; "
        "otherwise use province median when at least 30 observed values exist; "
        "otherwise fallback to the global median."
      ),
      "reason": (
        "Median imputation is robust to skew/outliers, while missing flags preserve the signal "
        "that the seller omitted a property attribute."
      ),
      "observed_missing_pct": {
        "bedrooms": float(missing_map.get("bedrooms", 0.0)),
        "bathrooms": float(missing_map.get("bathrooms", 0.0)),
        "floors": float(missing_map.get("floors", 0.0)),
      },
    },
    "outlier_strategy": {
      "decision": "keep_rows_use_flags_and_compare_with_robust_subset",
      "fact_table_policy": "Do not remove outliers from warehouse.fact_listings.",
      "feature_flag_columns": [
        "is_outlier_price",
        "is_outlier_area",
        "is_outlier_price_per_m2",
        "is_outlier_any",
      ],
      "robust_subset_column": "is_robust_train_candidate",
      "training_guidance": (
        "Use full-data baseline with outlier flags as model features, and compare it with a "
        "robust training subset where is_robust_train_candidate = true."
      ),
      "reason": (
        "This avoids silent data loss, keeps extreme listings auditable, and still enables "
        "robust-model experiments."
      ),
    },
  }
