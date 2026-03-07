"""Leakage-safe helpers for train/validation feature generation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class MarketReference:
  province_stats: pd.DataFrame
  district_stats: pd.DataFrame
  total_rows: int
  global_median_price: float
  global_median_price_per_m2: float


MARKET_REFERENCE_COLUMNS = [
  "province_listing_count",
  "district_listing_count",
  "province_frequency",
  "district_frequency",
  "province_median_price_million_vnd",
  "province_median_price_per_m2",
  "district_median_price_million_vnd",
  "district_median_price_per_m2",
  "district_to_province_price_ratio",
  "district_to_province_price_per_m2_ratio",
]


def _normalize_location(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()
  out["province"] = out["province"].fillna("Unknown").astype(str).str.strip()
  out["district"] = out["district"].fillna("Unknown").astype(str).str.strip()
  return out


def fit_market_reference(
  train_df: pd.DataFrame,
  target_col: str = "target_price_million_vnd",
  target_ppm2_col: str = "target_price_per_m2",
) -> MarketReference:
  train = _normalize_location(train_df)
  total_rows = len(train)

  province_stats = (
    train.groupby("province", as_index=False)
    .agg(
      province_listing_count=("listing_id", "count"),
      province_median_price_million_vnd=(target_col, "median"),
      province_median_price_per_m2=(target_ppm2_col, "median"),
    )
  )
  province_stats["province_frequency"] = province_stats["province_listing_count"] / max(total_rows, 1)

  district_stats = (
    train.groupby(["province", "district"], as_index=False)
    .agg(
      district_listing_count=("listing_id", "count"),
      district_median_price_million_vnd=(target_col, "median"),
      district_median_price_per_m2=(target_ppm2_col, "median"),
    )
  )
  district_stats["district_frequency"] = district_stats["district_listing_count"] / max(total_rows, 1)

  return MarketReference(
    province_stats=province_stats,
    district_stats=district_stats,
    total_rows=total_rows,
    global_median_price=float(train[target_col].median()),
    global_median_price_per_m2=float(train[target_ppm2_col].median()),
  )


def apply_market_reference(df: pd.DataFrame, reference: MarketReference) -> pd.DataFrame:
  out = _normalize_location(df)
  out = out.merge(reference.province_stats, on="province", how="left")
  out = out.merge(reference.district_stats, on=["province", "district"], how="left")

  out["province_listing_count"] = out["province_listing_count"].fillna(0).astype(int)
  out["district_listing_count"] = out["district_listing_count"].fillna(0).astype(int)
  out["province_frequency"] = out["province_frequency"].fillna(0.0)
  out["district_frequency"] = out["district_frequency"].fillna(0.0)
  out["province_median_price_million_vnd"] = out["province_median_price_million_vnd"].fillna(reference.global_median_price)
  out["province_median_price_per_m2"] = out["province_median_price_per_m2"].fillna(reference.global_median_price_per_m2)
  out["district_median_price_million_vnd"] = out["district_median_price_million_vnd"].fillna(out["province_median_price_million_vnd"])
  out["district_median_price_per_m2"] = out["district_median_price_per_m2"].fillna(out["province_median_price_per_m2"])
  out["district_to_province_price_ratio"] = out["district_median_price_million_vnd"] / out["province_median_price_million_vnd"].replace(0, pd.NA)
  out["district_to_province_price_per_m2_ratio"] = out["district_median_price_per_m2"] / out["province_median_price_per_m2"].replace(0, pd.NA)
  out["district_to_province_price_ratio"] = out["district_to_province_price_ratio"].fillna(1.0)
  out["district_to_province_price_per_m2_ratio"] = out["district_to_province_price_per_m2_ratio"].fillna(1.0)
  return out


def build_oof_market_features(
  train_df: pd.DataFrame,
  n_splits: int = 5,
  random_state: int = 42,
) -> pd.DataFrame:
  if n_splits < 2:
    raise ValueError("n_splits must be at least 2 for out-of-fold features.")
  if len(train_df) < n_splits:
    raise ValueError("n_splits cannot exceed the number of training rows.")

  shuffled_index = pd.Index(train_df.index.to_list())
  rng = np.random.default_rng(random_state)
  shuffled_positions = rng.permutation(len(shuffled_index))
  fold_ids = pd.Series(shuffled_positions % n_splits, index=shuffled_index[shuffled_positions])

  oof_frames: list[pd.DataFrame] = []
  for fold in range(n_splits):
    holdout_index = fold_ids[fold_ids == fold].index
    fold_train = train_df.drop(index=holdout_index)
    fold_valid = train_df.loc[holdout_index]
    reference = fit_market_reference(fold_train)
    enriched = apply_market_reference(fold_valid, reference)
    oof_frames.append(enriched[MARKET_REFERENCE_COLUMNS])

  oof_df = pd.concat(oof_frames).sort_index()
  return oof_df.reindex(train_df.index)


def select_predictor_columns(include_optional: bool = True) -> list[str]:
  columns = [
    "province",
    "district",
    "timeline_bucket",
    "area_m2",
    "area_m2_capped_p99",
    "log_area_m2",
    "bedrooms_imputed",
    "bedrooms_missing",
    "bathrooms_imputed",
    "bathrooms_missing",
    "floors_imputed",
    "floors_missing",
    "has_frontage",
    "timeline_hours_imputed",
    "timeline_log_hours",
    "is_new_listing",
    "total_rooms",
    "area_per_room",
    "bedroom_density",
    "bathroom_density",
    "is_large_property",
    "is_multi_floor",
  ]
  if include_optional:
    columns.extend(
      [
        "province_frequency",
        "district_frequency",
        "province_listing_count",
        "district_listing_count",
        "province_median_price_million_vnd",
        "province_median_price_per_m2",
        "district_median_price_million_vnd",
        "district_median_price_per_m2",
        "district_to_province_price_ratio",
        "district_to_province_price_per_m2_ratio",
        "has_frontage_x_log_area",
        "is_outlier_area",
      ]
    )
  return columns


def prepare_tree_model_frame(
  base_df: pd.DataFrame,
  reference: MarketReference,
  include_optional: bool = True,
) -> tuple[pd.DataFrame, pd.Series, list[str]]:
  prepared = apply_market_reference(base_df, reference)
  feature_cols = select_predictor_columns(include_optional=include_optional)
  model_df = prepared[feature_cols].copy()
  categorical_cols = ["province", "district", "timeline_bucket"]
  for col in categorical_cols:
    model_df[col] = model_df[col].astype("category")
  target = prepared["target_log_price_million_vnd"].copy()
  return model_df, target, categorical_cols


def prepare_linear_model_frame(
  base_df: pd.DataFrame,
  reference: MarketReference,
  keep_top_districts: int = 50,
  include_optional: bool = True,
) -> tuple[pd.DataFrame, pd.Series]:
  prepared = apply_market_reference(base_df, reference)
  frequent_districts = (
    prepared["district"].value_counts().head(keep_top_districts).index.tolist()
  )
  prepared = prepared.copy()
  prepared["district_for_ohe"] = prepared["district"].where(prepared["district"].isin(frequent_districts), "Other")

  numeric_cols = [col for col in select_predictor_columns(include_optional=include_optional) if col not in {"province", "district", "timeline_bucket"}]
  design = prepared[numeric_cols + ["province", "district_for_ohe", "timeline_bucket"]].copy()
  design = pd.get_dummies(design, columns=["province", "district_for_ohe", "timeline_bucket"], dummy_na=False)
  target = prepared["target_log_price_million_vnd"].copy()
  return design, target


def describe_leakage_safe_workflow() -> list[str]:
  return [
    "Split data into train/validation/test before fitting location encoders or market aggregates.",
    "For rows inside the training split, build province/district market aggregates with out-of-fold logic.",
    "Fit province/district counts and median price references on training rows only.",
    "Apply the fitted reference tables to validation/test rows via left joins on province/district.",
    "Do not use target-derived outlier flags as predictors; keep them only for filtering experiments or analysis.",
  ]
