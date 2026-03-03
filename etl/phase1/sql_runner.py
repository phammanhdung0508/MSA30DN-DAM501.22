#!/usr/bin/env python3
"""SQL file execution helpers."""

from __future__ import annotations

from pathlib import Path


def run_sql_file(engine, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)
