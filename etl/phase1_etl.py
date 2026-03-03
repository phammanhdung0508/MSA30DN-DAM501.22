#!/usr/bin/env python3
"""Backward-compatible entrypoint for refactored Phase 1 ETL."""

from __future__ import annotations

try:
    from .phase1.main import main
except ImportError:
    from phase1.main import main


if __name__ == "__main__":
    main()
