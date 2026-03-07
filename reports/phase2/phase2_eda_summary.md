# Phase 2 EDA Summary

## 1) Data Quality Validation

- Row count in `warehouse.fact_listings`: **49,955**
- Duplicate `listing_id` rows: **0**
- Duplicate dedupe-key rows: **0**
- Missing summary table: `reports/phase2/tables/quality_missing_summary.csv`
- Numeric range table: `reports/phase2/tables/quality_numeric_ranges.csv`

## 2) Target Variable Analysis

- Price skewness: **11.6437**
- Log-price skewness: **-1.5776**
- Price-per-m2 skewness: **128.7041**
- Log transform recommended: **True**
- Distribution figure: `reports/phase2/figures/target_distributions.png`

## 3) Location-Based Market Analysis

- Province ranking table: `reports/phase2/tables/location_by_province.csv`
- District ranking table: `reports/phase2/tables/location_by_district.csv`
- Location overview figure: `reports/phase2/figures/location_market_overview.png`

## 4) Property Size Analysis

- Corr(area, price): **0.0705**
- Corr(area, price_per_m2): **-0.0027**
- Scatter figure: `reports/phase2/figures/size_relationship_scatter.png`
- Outlier candidates table: `reports/phase2/tables/size_outlier_candidates.csv`

## 5) Property Feature Analysis

- Bedrooms stats: `reports/phase2/tables/feature_bedrooms_stats.csv`
- Floors stats: `reports/phase2/tables/feature_floors_stats.csv`
- Feature figure: `reports/phase2/figures/property_feature_analysis.png`

## 6) Correlation Analysis

- Correlation matrix table: `reports/phase2/tables/correlation_matrix.csv`
- Heatmap: `reports/phase2/figures/correlation_heatmap.png`
- Strong predictors (|corr|>=0.2) vs target:
  - bathrooms: 0.3244
  - bedrooms: 0.3099

## 7) Timeline Analysis

- Timeline bucket table: `reports/phase2/tables/timeline_bucket_summary.csv`
- Timeline figure: `reports/phase2/figures/timeline_analysis.png`

## 8) Market Analytics Mart Validation

- Compared groups: **472**
- Mismatch groups: **0**
- Mismatch detail table: `reports/phase2/tables/mart_market_analytics_mismatches.csv`

## 9) Modeling Decisions

- Missing strategy:
  - Use raw columns for audit, but model with bedrooms_imputed, bathrooms_imputed, floors_imputed plus bedrooms_missing, bathrooms_missing, floors_missing.
  - Rule: Use district median when at least 30 observed values exist for the feature; otherwise use province median when at least 30 observed values exist; otherwise fallback to the global median.
- Outlier strategy:
  - Keep all rows in `warehouse.fact_listings` and `warehouse.mart_avm_features`; do not hard-delete outliers.
  - Use is_outlier_price, is_outlier_area, is_outlier_price_per_m2, is_outlier_any as features.
  - Optional robust subset: `is_robust_train_candidate`.

## 10) AVM Feature Recommendations

| feature | missing_pct | reason |
|---|---:|---|
| area_m2 | 0.00% | Strong structural size signal for total price. |
| bedrooms_imputed | 0.00% | Location-aware median-imputed bedroom count for modeling. |
| bathrooms_imputed | 0.00% | Location-aware median-imputed bathroom count for modeling. |
| floors_imputed | 0.00% | Location-aware median-imputed floor count for modeling. |
| bedrooms_missing | 0.00% | Preserves the information that bedroom count was not disclosed. |
| bathrooms_missing | 0.00% | Preserves the information that bathroom count was not disclosed. |
| floors_missing | 0.00% | Preserves the information that floor count was not disclosed. |
| frontage | 0.00% | Commercial exposure and access proxy. |
| province | 0.00% | Macro regional price level. |
| district | 0.00% | Micro location premium. |
| timeline_hours | 0.00% | Listing freshness and pricing behavior. |
| district_median_price_million_vnd | 0.00% | Contextual market baseline by district. |
| district_median_price_per_m2 | 0.00% | Location-normalized pricing baseline. |
| is_outlier_price | 0.00% | Flags atypical total-price observations for robust modeling decisions. |
| is_outlier_area | 0.00% | Flags atypical property size observations. |
| is_outlier_price_per_m2 | 0.00% | Flags atypical unit-price observations. |
| is_outlier_any | 0.00% | Compact indicator that at least one outlier rule fired. |

## Key Insights (5-10)

1. Price distribution is right-skewed (skew=11.64), and log(price+1) reduces skew to -1.58.
2. Feature completeness is uneven: bedrooms/bathrooms/floors missing rates are 10.85%, 16.40%, and 21.82%.
3. Area has a positive relation with total price (corr=0.070), while relation with price_per_m2 is weaker (corr=-0.003).
4. Province-level market spread is large; top provinces by median price_per_m2 are significantly above the national median.
5. Timeline is highly imbalanced, with most listings in >30 days bucket; this can bias recency-based interpretation.
6. No duplicate listing_id or dedupe_key remains in fact_listings, indicating Phase 1 dedup logic is stable on current data.
7. Correlation matrix highlights candidate predictors for AVM from numeric block: bathrooms (0.32), bedrooms (0.31)
8. Market mart consistency check shows 0 mismatched groups over 472 compared groups.