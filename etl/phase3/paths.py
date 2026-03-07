"""Filesystem paths and lightweight IO helpers for Phase 3."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class Paths:
  base: Path
  tables: Path
  summary_md: Path
  summary_json: Path


def build_paths(output_dir: str) -> Paths:
  base = Path(output_dir)
  tables = base / "tables"
  tables.mkdir(parents=True, exist_ok=True)
  return Paths(
    base=base,
    tables=tables,
    summary_md=base / "phase3_feature_engineering_summary.md",
    summary_json=base / "phase3_feature_engineering_summary.json",
  )


def save_table(df: pd.DataFrame, path: Path) -> None:
  df.to_csv(path, index=False)
