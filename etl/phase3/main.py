"""Pipeline orchestration for Phase 3 feature engineering."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine

try:
  from etl.phase1.sql_runner import run_sql_file
except ImportError:
  from phase1.sql_runner import run_sql_file

from .config import parse_args
from .extract import load_summary_metrics
from .paths import build_paths
from .reporting import build_feature_review_tables, write_summary


def main() -> None:
  args = parse_args()
  if not args.db_uri:
    raise SystemExit("Missing DB URI. Set --db-uri or DW_PG_URI.")

  engine = create_engine(args.db_uri, future=True)
  sql_dir = Path(args.sql_dir)
  paths = build_paths(args.output_dir)

  for ddl_file in [
    sql_dir / "01_create_feature_mart.sql",
    sql_dir / "02_refresh_feature_mart.sql",
  ]:
    run_sql_file(engine, ddl_file)

  metrics = load_summary_metrics(engine)
  artifacts = build_feature_review_tables(paths)
  write_summary(paths, metrics=metrics, artifacts=artifacts)

  print("Phase 3 feature engineering completed.")
  print(f"Summary markdown: {paths.summary_md}")
  print(f"Summary JSON: {paths.summary_json}")


if __name__ == "__main__":
  main()
