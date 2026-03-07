"""Target-variable distribution analysis for Phase 2 EDA."""

from __future__ import annotations

from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .paths import Paths


def analyze_target_distribution(fact: pd.DataFrame, paths: Paths) -> dict[str, Any]:
  working = fact.copy()
  working["log_price_million_vnd"] = np.log1p(working["price_million_vnd"])

  stats = {
    "price_skewness": float(working["price_million_vnd"].skew()),
    "log_price_skewness": float(working["log_price_million_vnd"].skew()),
    "price_per_m2_skewness": float(working["price_per_m2"].skew()),
  }

  fig, axes = plt.subplots(1, 3, figsize=(18, 5))
  sns.histplot(working["price_million_vnd"], bins=60, ax=axes[0], color="#2C7FB8")
  axes[0].set_title("Price Distribution")
  axes[0].set_xlabel("price_million_vnd")

  sns.histplot(working["log_price_million_vnd"], bins=60, ax=axes[1], color="#7FCDBB")
  axes[1].set_title("Log Price Distribution")
  axes[1].set_xlabel("log(price_million_vnd + 1)")

  sns.histplot(working["price_per_m2"], bins=60, ax=axes[2], color="#F03B20")
  axes[2].set_title("Price per m2 Distribution")
  axes[2].set_xlabel("price_per_m2")

  fig.tight_layout()
  out_path = paths.figures / "target_distributions.png"
  fig.savefig(out_path, dpi=180)
  plt.close(fig)

  return {
    **stats,
    "figure": str(out_path),
    "log_transform_recommended": bool(stats["log_price_skewness"] < stats["price_skewness"]),
  }
