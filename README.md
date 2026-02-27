# Kaggle Dataset Downloader

This project downloads a Kaggle dataset with `kagglehub` into a local `data/` folder.

## Environment

Use Conda env `p31114`:

```bash
conda run -n p31114 python -V
```

## Install dependency

```bash
conda run -n p31114 python -m pip install kagglehub
```

## Kaggle authentication

If authentication is required, provide credentials using one of these:

1. `~/.kaggle/kaggle.json` (recommended by Kaggle)
2. Environment variables: `KAGGLE_USERNAME` and `KAGGLE_KEY`

You can also keep a local `kaggle.json` in this repo for convenience, but do not commit it.

## Download dataset

Default dataset and output folder:

```bash
conda run -n p31114 python download_kaggle_dataset.py
```

Custom dataset and output folder:

```bash
conda run -n p31114 python download_kaggle_dataset.py \
  --dataset owner/dataset-name \
  --outdir data/raw
```

By default, files are copied to `--outdir/<owner_dataset>/` to avoid overwrite.

If you want to copy directly into `--outdir`:

```bash
conda run -n p31114 python download_kaggle_dataset.py \
  --dataset owner/dataset-name \
  --outdir data/raw \
  --no-dataset-subdir
```

## Data quality audit

Use `audit_data_quality.py` to run reusable checks for:

- Relevance
- Accessibility
- Interpretability
- Reliability
- Timeliness
- Accuracy
- Consistency
- Precision
- Granularity

The script now supports multiple raw schemas:

- `cresht2606` style: `id, detail_url, title, location, timeline_hours, area_m2, ...`
- `andyvo1009` style: `product_id, address, price, area, bedrooms_num, bathrooms_num`

For `andyvo1009` style files, it auto-normalizes:

- `product_id -> id`
- `address -> location`
- `area` text (e.g. `110 m²`) -> `area_m2`
- `price` text (e.g. `26 triệu/tháng`, `22,96 tỷ`) -> `price_million_vnd`

### Quick run

```bash
conda run -n p31114 python audit_data_quality.py \
  --input-dir data/raw/cresht2606_vietnam-real-estate-datasets-catalyst
```

### Save full report to JSON

```bash
conda run -n p31114 python audit_data_quality.py \
  --input-dir data/raw/cresht2606_vietnam-real-estate-datasets-catalyst \
  --output-json data/processed/cresht2606_quality_report.json
```

Run for `andyvo1009` dataset:

```bash
conda run -n p31114 python audit_data_quality.py \
  --input-dir data/raw/andyvo1009_real-estate-in-vietnam \
  --output-json data/processed/andyvo1009_quality_report.json
```

### Useful options

```bash
conda run -n p31114 python audit_data_quality.py --help
```

Key options:

- `--stale-hours`: threshold for old listings (default `8760` = 1 year)
- `--sample-limit`: number of sample rows per outlier rule
- `--output-json`: path for saving full metrics, samples, and criteria statuses

## Regression-based cleaning (for cresht2606)

Use regression residuals to flag price outliers and export cleaned data.

```bash
conda run -n p31114 python clean_cresht2606_regression.py \
  --input-dir data/raw/cresht2606_vietnam-real-estate-datasets-catalyst \
  --output-dir data/processed
```

Main outputs:

- `data/processed/cresht2606_regression_cleaned.csv`
- `data/processed/cresht2606_regression_flags.csv`
- `data/processed/cresht2606_regression_cleaning_summary.json`

# Trend and Evolution Analysis