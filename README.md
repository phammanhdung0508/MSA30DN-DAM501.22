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
  --outdir data
```
