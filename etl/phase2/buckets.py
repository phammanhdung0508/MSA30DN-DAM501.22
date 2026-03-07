"""Timeline bucket helpers for user-facing and mart-aligned analysis."""

from __future__ import annotations

from typing import Any

import pandas as pd


def bucket_timeline_user(value: Any) -> str:
  if pd.isna(value):
    return "unknown"
  hours = float(value)
  if hours < 24:
    return "0_24h"
  if hours < 72:
    return "1_3d"
  if hours < 168:
    return "3_7d"
  if hours < 720:
    return "7_30d"
  return "gt_30d"


def bucket_timeline_mart(value: Any) -> str:
  if pd.isna(value):
    return "unknown"
  hours = float(value)
  if hours < 24:
    return "0_24h"
  if hours < 72:
    return "24_72h"
  if hours < 168:
    return "3_7d"
  if hours < 720:
    return "8_30d"
  return "gt_30d"
