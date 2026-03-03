import types
import unittest
from unittest.mock import patch

import pandas as pd

from etl.phase1.quality import QualityGateError


class MainFlowTests(unittest.TestCase):
    def _args(self, **overrides):
        base = {
            "db_uri": "postgresql+psycopg2://postgres:postgres@localhost:5432/DAM501.22",
            "csv_path": "dummy.csv",
            "sql_dir": "sql/phase1",
            "skip_marts": False,
            "skip_quality_gate": False,
            "max_null_price_ratio": 0.10,
            "max_invalid_price_ratio": 0.02,
            "max_null_area_ratio": 0.35,
            "max_invalid_area_ratio": 0.01,
            "max_duplicate_detail_url_ratio": 0.01,
            "min_fact_row_ratio": 0.60,
        }
        base.update(overrides)
        return types.SimpleNamespace(**base)

    def test_quality_gate_failure_still_lands_raw_and_staging(self) -> None:
        raw_df = pd.DataFrame({"id": ["1"]})
        staging_df = pd.DataFrame({"id": ["1"], "detail_url": ["a"], "price_million_vnd": [1], "area_m2": [1]})
        fact_df = pd.DataFrame({"listing_id": ["1"]})

        call_order: list[str] = []

        def _record(name):
            def _inner(*args, **kwargs):
                call_order.append(name)

            return _inner

        with patch("etl.phase1.main.parse_args", return_value=self._args()), patch(
            "etl.phase1.main.create_engine", return_value=object()
        ), patch("etl.phase1.main.run_sql_file", side_effect=_record("run_sql_file")), patch(
            "etl.phase1.main.start_etl_run", return_value=999
        ), patch("etl.phase1.main.read_raw_text_dataframe", return_value=raw_df), patch(
            "etl.phase1.main.build_raw_landing_dataframe", return_value=raw_df
        ), patch("etl.phase1.main.build_staging_dataframe", return_value=staging_df), patch(
            "etl.phase1.main.build_fact_dataframe", return_value=fact_df
        ), patch("etl.phase1.main.compute_quality_metrics", return_value={"fact_row_ratio": 0.1}), patch(
            "etl.phase1.main.enforce_quality_gate", side_effect=QualityGateError("gate fail")
        ), patch("etl.phase1.main.load_raw_landing", side_effect=_record("load_raw_landing")), patch(
            "etl.phase1.main.load_staging", side_effect=_record("load_staging")
        ), patch("etl.phase1.main.load_fact", side_effect=_record("load_fact")), patch(
            "etl.phase1.main.refresh_location_dimension", side_effect=_record("refresh_location_dimension")
        ), patch("etl.phase1.main.fetch_count", return_value=1), patch(
            "etl.phase1.main.finish_etl_run"
        ) as finish_mock:
            from etl.phase1.main import main

            with self.assertRaises(QualityGateError):
                main()

            self.assertIn("load_raw_landing", call_order)
            self.assertIn("load_staging", call_order)
            self.assertNotIn("load_fact", call_order)

            finish_mock.assert_called_once()
            kwargs = finish_mock.call_args.kwargs
            self.assertEqual(kwargs["status"], "failed")
            self.assertEqual(kwargs["run_id"], 999)


if __name__ == "__main__":
    unittest.main()
