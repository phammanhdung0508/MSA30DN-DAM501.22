#!/usr/bin/env python3
import argparse
import os
import shutil
import sys

import kagglehub

# https://www.kaggle.com/datasets/andyvo1009/real-estate-in-vietnam
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a Kaggle dataset using kagglehub into a local folder."
    )
    parser.add_argument(
        "--dataset",
        default="andyvo1009/real-estate-in-vietnam",
        help="Kaggle dataset handle, e.g. owner/dataset-name",
    )
    parser.add_argument(
        "--outdir",
        default="data/raw",
        help="Destination folder to copy dataset files into.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    try:
        source_dir = kagglehub.dataset_download(args.dataset)
    except Exception as exc:
        print(f"Download failed for '{args.dataset}': {exc}", file=sys.stderr)
        print(
            "If this dataset is private or access is denied, configure Kaggle auth "
            "via ~/.kaggle/kaggle.json (or KAGGLE_USERNAME/KAGGLE_KEY).",
            file=sys.stderr,
        )
        return 1

    shutil.copytree(source_dir, outdir, dirs_exist_ok=True)

    files = sorted(os.listdir(outdir))
    print(f"Downloaded cache path: {source_dir}")
    print(f"Copied dataset to: {outdir}")
    print(f"Files in output: {files}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
