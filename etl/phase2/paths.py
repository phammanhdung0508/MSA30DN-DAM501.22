"""Filesystem paths and lightweight IO helpers for Phase 2 EDA."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class Paths:
  base: Path
  figures: Path
  tables: Path
  summary_md: Path
  summary_json: Path


def build_paths(output_dir: str) -> Paths:
  base = Path(output_dir)
  figures = base / "figures"
  tables = base / "tables"
  figures.mkdir(parents=True, exist_ok=True)
  tables.mkdir(parents=True, exist_ok=True)
  return Paths(
    base=base,
    figures=figures,
    tables=tables,
    summary_md=base / "phase2_eda_summary.md",
    summary_json=base / "phase2_eda_summary.json",
  )


def save_table(df: pd.DataFrame, path: Path) -> None:
  df.to_csv(path, index=False)
