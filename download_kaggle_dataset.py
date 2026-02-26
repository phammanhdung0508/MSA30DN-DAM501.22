#!/usr/bin/env python3
import argparse
import os
import shutil
import sys

import kagglehub


def dataset_to_folder_name(dataset_handle: str) -> str:
    return dataset_handle.replace("/", "_")


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
        help="Base output folder.",
    )
    parser.add_argument(
        "--no-dataset-subdir",
        action="store_true",
        help=(
            "Copy files directly into --outdir. "
            "Default behavior is --outdir/<owner_dataset>/"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_outdir = os.path.abspath(args.outdir)
    if args.no_dataset_subdir:
        outdir = base_outdir
    else:
        outdir = os.path.join(base_outdir, dataset_to_folder_name(args.dataset))
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
    print(f"Dataset handle: {args.dataset}")
    print(f"Copied dataset to: {outdir}")
    print(f"Files in output: {files}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
