"""Backward-compatible entrypoint for refactored Phase 3 feature engineering."""

from __future__ import annotations

try:
  from .phase3.main import main
except ImportError:
  from phase3.main import main


if __name__ == "__main__":
  main()
