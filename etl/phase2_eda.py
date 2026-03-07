#!/usr/bin/env python3
"""Backward-compatible entrypoint for refactored Phase 2 EDA."""

from __future__ import annotations

try:
  from .phase2.main import main
except ImportError:
  from phase2.main import main


if __name__ == "__main__":
  main()
