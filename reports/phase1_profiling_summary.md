# Phase 1 Data Profiling Summary

- Input CSV: `data/raw/cresht2606_vietnam-real-estate-datasets-catalyst/house_buying_dec29th_2025.csv`
- Row count: **66,912**
- Column count: **11**

## Missing Values

| column | missing_count | missing_ratio |
|---|---:|---:|
| id | 0 | 0.00% |
| detail_url | 7 | 0.01% |
| title | 0 | 0.00% |
| location | 0 | 0.00% |
| timeline_hours | 0 | 0.00% |
| area_m2 | 16,568 | 24.76% |
| bedrooms | 11,639 | 17.39% |
| bathrooms | 15,364 | 22.96% |
| floors | 14,634 | 21.87% |
| frontage | 0 | 0.00% |
| price_million_vnd | 2,103 | 3.14% |

## Duplicates

- Duplicate rows by `id` (all repeated rows): **0**
- Duplicate keys by `id`: **0**
- Duplicate rows by `detail_url` (including empty/null): **7**
- Duplicate keys by `detail_url` (including empty/null): **1**
- Duplicate rows by non-empty `detail_url` (all repeated rows): **0**
- Duplicate keys by non-empty `detail_url`: **0**
- Exact full-row duplicates: **0**

## Invalid Value Checks

- `price_million_vnd <= 0`: **362**
- `area_m2 <= 0`: **0**
- `price_per_m2 <= 0` (computed): **360**

## Distribution Summary

### price_million_vnd

- count=64,809, min=0.0, p50=7000.0, p95=38500.0, p99=130000.0, max=989000.0, mean=13810.677154546436
- IQR outliers: 5,837 rows (9.01%), bounds=(-7700.0, 25100.0)

### area_m2

- count=50,344, min=1.0, p50=60.0, p95=237.0, p99=955.6999999999971, max=62000000.0, mean=1377.9767003019228
- IQR outliers: 4,288 rows (8.52%), bounds=(-33.0, 167.0)

### price_per_m2

- count=50,315, min=0.0, p50=139.1304347826087, p95=428.57142857142856, p99=893.266666666667, max=406000.0, mean=213.00771617376097
- IQR outliers: 2,491 rows (4.95%), bounds=(-127.34420289855073, 430.018115942029)

## Location Format Analysis

- Unique raw location strings: **280**
- Unique raw provinces: **57**
- Unique raw districts: **274**
- Unique normalized provinces (accent removed): **57**
- Unique normalized districts (accent removed): **273**

### Segment Distribution

- 2 segment(s): 66,912

### Top 10 Raw Locations

1. Gò Vấp, Hồ Chí Minh: 3,441
2. Đống Đa, Hà Nội: 3,252
3. Tân Phú, Hồ Chí Minh: 3,247
4. Bình Tân, Hồ Chí Minh: 3,130
5. Long Biên, Hà Nội: 3,111
6. Quận 12, Hồ Chí Minh: 2,765
7. Thanh Xuân, Hà Nội: 2,670
8. Cầu Giấy, Hà Nội: 2,666
9. Tân Bình, Hồ Chí Minh: 2,659
10. Hoàng Mai, Hà Nội: 2,506

## Recommended Cleaning Rules

- Remove rows where `price_million_vnd <= 0`.
- Remove rows where `area_m2 <= 0`.
- Compute `price_per_m2 = price_million_vnd / area_m2`.
- De-duplicate by non-empty `detail_url`; fallback key `id` when URL missing.
- Standardize location by splitting `district, province`, removing accents, and normalizing admin prefixes.
- Keep `timeline_hours` as feature and derive time buckets for marts.
