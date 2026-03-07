"""Shared constants for Phase 2 EDA."""

from __future__ import annotations

FACT_QUERY = """
SELECT
  listing_id,
  detail_url,
  title,
  province,
  district,
  timeline_hours,
  area_m2,
  bedrooms,
  bathrooms,
  floors,
  frontage,
  price_million_vnd,
  price_per_m2
FROM warehouse.fact_listings
"""

MART_QUERY = """
SELECT
  province,
  district,
  time_bucket,
  median_price_million_vnd,
  median_price_per_m2,
  listing_count
FROM warehouse.mart_market_analytics
"""

KEY_COLUMNS = [
  "price_million_vnd",
  "area_m2",
  "bedrooms",
  "bathrooms",
  "floors",
  "frontage",
  "province",
  "district",
]

NUMERIC_COLUMNS = [
  "price_million_vnd",
  "area_m2",
  "bedrooms",
  "bathrooms",
  "floors",
  "frontage",
  "price_per_m2",
]
