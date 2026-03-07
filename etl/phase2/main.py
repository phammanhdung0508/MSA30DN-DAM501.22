"""Pipeline orchestration for Phase 2 EDA."""

from __future__ import annotations

import json

import seaborn as sns

from .analysis_features import (
  analyze_correlations,
  analyze_property_features,
  analyze_property_size,
  build_modeling_strategy,
  recommend_avm_features,
)
from .analysis_market import analyze_location_market, analyze_timeline, validate_market_mart
from .analysis_quality import analyze_data_quality
from .analysis_target import analyze_target_distribution
from .config import parse_args
from .extract import load_data
from .paths import build_paths
from .reporting import build_key_insights, write_markdown_report


def main() -> None:
  args = parse_args()
  if not args.db_uri:
    raise SystemExit("Missing DB URI. Set --db-uri or DW_PG_URI.")

  sns.set_theme(style="whitegrid")
  paths = build_paths(args.output_dir)

  fact, mart = load_data(args.db_uri)

  quality = analyze_data_quality(fact, paths)
  target = analyze_target_distribution(fact, paths)
  location = analyze_location_market(fact, paths, min_district_listings=args.min_district_listings)
  size = analyze_property_size(fact, paths, max_scatter_points=args.max_scatter_points)
  property_features = analyze_property_features(fact, paths)
  corr = analyze_correlations(fact, paths)
  timeline = analyze_timeline(fact, paths)
  mart_check = validate_market_mart(fact, mart, paths)
  modeling_strategy = build_modeling_strategy(quality)
  avm_features = recommend_avm_features(quality, corr)

  insights = build_key_insights(
    quality=quality,
    target=target,
    size=size,
    corr=corr,
    mart_check=mart_check,
  )

  report_features = {
    **avm_features,
    "bedrooms_stats_csv": property_features["bedrooms_stats_csv"],
    "floors_stats_csv": property_features["floors_stats_csv"],
    "figure": property_features["figure"],
  }

  write_markdown_report(
    paths=paths,
    quality=quality,
    target=target,
    location=location,
    size=size,
    features=report_features,
    corr=corr,
    timeline=timeline,
    mart_check=mart_check,
    modeling_strategy=modeling_strategy,
    insights=insights,
  )

  summary_payload = {
    "quality": quality,
    "target": target,
    "location": location,
    "size": size,
    "property_features": property_features,
    "correlation": corr,
    "timeline": timeline,
    "mart_validation": mart_check,
    "modeling_strategy": modeling_strategy,
    "avm_feature_recommendation": avm_features,
    "key_insights": insights,
  }
  paths.summary_json.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

  print(f"Phase 2 EDA completed. Summary markdown: {paths.summary_md}")
  print(f"Summary JSON: {paths.summary_json}")


if __name__ == "__main__":
  main()
