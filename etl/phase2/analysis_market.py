"""Location, timeline, and mart-consistency analysis for Phase 2 EDA."""

from __future__ import annotations

from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from .buckets import bucket_timeline_mart, bucket_timeline_user
from .paths import Paths, save_table


def analyze_location_market(
  fact: pd.DataFrame,
  paths: Paths,
  min_district_listings: int,
) -> dict[str, Any]:
  province_df = (
    fact.groupby("province", as_index=False)
    .agg(
      listing_count=("listing_id", "count"),
      median_price_million_vnd=("price_million_vnd", "median"),
      median_price_per_m2=("price_per_m2", "median"),
    )
    .sort_values(["median_price_per_m2", "listing_count"], ascending=[False, False])
  )
  save_table(province_df, paths.tables / "location_by_province.csv")

  district_df = (
    fact.groupby(["province", "district"], as_index=False)
    .agg(
      listing_count=("listing_id", "count"),
      median_price_per_m2=("price_per_m2", "median"),
      median_price_million_vnd=("price_million_vnd", "median"),
    )
    .query("listing_count >= @min_district_listings")
    .sort_values(["median_price_per_m2", "listing_count"], ascending=[False, False])
  )
  save_table(district_df, paths.tables / "location_by_district.csv")

  top_count = province_df.sort_values("listing_count", ascending=False).head(12)
  top_ppm2 = province_df.sort_values("median_price_per_m2", ascending=False).head(12)

  fig, axes = plt.subplots(1, 2, figsize=(18, 6))
  sns.barplot(data=top_count, y="province", x="listing_count", ax=axes[0], color="#4EB3D3")
  axes[0].set_title("Top Provinces by Listing Count")

  sns.barplot(data=top_ppm2, y="province", x="median_price_per_m2", ax=axes[1], color="#F16913")
  axes[1].set_title("Top Provinces by Median Price per m2")

  fig.tight_layout()
  out_path = paths.figures / "location_market_overview.png"
  fig.savefig(out_path, dpi=180)
  plt.close(fig)

  return {
    "province_table_csv": str(paths.tables / "location_by_province.csv"),
    "district_table_csv": str(paths.tables / "location_by_district.csv"),
    "figure": str(out_path),
    "top_province_by_listing_count": top_count.iloc[0].to_dict() if not top_count.empty else None,
    "top_province_by_median_price_per_m2": top_ppm2.iloc[0].to_dict() if not top_ppm2.empty else None,
  }


def analyze_timeline(fact: pd.DataFrame, paths: Paths) -> dict[str, Any]:
  timeline_df = fact.copy()
  timeline_df["time_bucket"] = timeline_df["timeline_hours"].map(bucket_timeline_user)

  order = ["0_24h", "1_3d", "3_7d", "7_30d", "gt_30d", "unknown"]
  agg = (
    timeline_df.groupby("time_bucket", as_index=False)
    .agg(
      listing_count=("listing_id", "count"),
      median_price_million_vnd=("price_million_vnd", "median"),
      median_price_per_m2=("price_per_m2", "median"),
    )
  )
  agg["order"] = agg["time_bucket"].map({label: i for i, label in enumerate(order)})
  agg = agg.sort_values("order").drop(columns=["order"])
  save_table(agg, paths.tables / "timeline_bucket_summary.csv")

  fig, axes = plt.subplots(1, 2, figsize=(16, 5))
  sns.barplot(data=agg, x="time_bucket", y="listing_count", ax=axes[0], color="#41AB5D")
  axes[0].set_title("Listing Count by Time Bucket")
  axes[0].tick_params(axis="x", rotation=20)

  sns.barplot(data=agg, x="time_bucket", y="median_price_million_vnd", ax=axes[1], color="#DD1C77")
  axes[1].set_title("Median Price by Time Bucket")
  axes[1].tick_params(axis="x", rotation=20)

  fig.tight_layout()
  out_path = paths.figures / "timeline_analysis.png"
  fig.savefig(out_path, dpi=180)
  plt.close(fig)

  return {
    "timeline_table_csv": str(paths.tables / "timeline_bucket_summary.csv"),
    "figure": str(out_path),
  }


def validate_market_mart(fact: pd.DataFrame, mart: pd.DataFrame, paths: Paths) -> dict[str, Any]:
  rebuilt = fact.copy()
  rebuilt["time_bucket"] = rebuilt["timeline_hours"].map(bucket_timeline_mart)
  rebuilt = (
    rebuilt.groupby(["province", "district", "time_bucket"], as_index=False)
    .agg(
      listing_count=("listing_id", "count"),
      median_price_million_vnd=("price_million_vnd", "median"),
      median_price_per_m2=("price_per_m2", "median"),
    )
    .rename(
      columns={
        "listing_count": "fact_listing_count",
        "median_price_million_vnd": "fact_median_price_million_vnd",
        "median_price_per_m2": "fact_median_price_per_m2",
      }
    )
  )

  mart_renamed = mart.rename(
    columns={
      "listing_count": "mart_listing_count",
      "median_price_million_vnd": "mart_median_price_million_vnd",
      "median_price_per_m2": "mart_median_price_per_m2",
    }
  )

  merged = rebuilt.merge(
    mart_renamed,
    on=["province", "district", "time_bucket"],
    how="outer",
  )

  merged["listing_count_diff"] = merged["mart_listing_count"].fillna(-1) - merged["fact_listing_count"].fillna(-1)
  merged["median_price_diff"] = (
    merged["mart_median_price_million_vnd"].fillna(-1.0) - merged["fact_median_price_million_vnd"].fillna(-1.0)
  )
  merged["median_ppm2_diff"] = merged["mart_median_price_per_m2"].fillna(-1.0) - merged["fact_median_price_per_m2"].fillna(-1.0)

  mismatch_mask = (
    (merged["listing_count_diff"] != 0)
    | (merged["median_price_diff"].abs() > 1e-9)
    | (merged["median_ppm2_diff"].abs() > 1e-9)
  )
  mismatches = merged[mismatch_mask].copy()
  save_table(mismatches, paths.tables / "mart_market_analytics_mismatches.csv")

  return {
    "compared_group_count": int(len(merged)),
    "mismatch_group_count": int(len(mismatches)),
    "mismatch_table_csv": str(paths.tables / "mart_market_analytics_mismatches.csv"),
  }
