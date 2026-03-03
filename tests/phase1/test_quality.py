import unittest

import pandas as pd

from etl.phase1.quality import QualityGateError, QualityThresholds, compute_quality_metrics, enforce_quality_gate


class QualityGateTests(unittest.TestCase):
    def test_compute_quality_metrics(self) -> None:
        staging_df = pd.DataFrame(
            {
                "detail_url": ["a", "", "a", "b"],
                "price_million_vnd": [1000, None, 0, 2000],
                "area_m2": [10, None, 20, 0],
            }
        )
        fact_df = pd.DataFrame({"listing_id": ["1", "2"]})

        metrics = compute_quality_metrics(staging_df, fact_df)

        self.assertEqual(metrics["total_rows"], 4)
        self.assertEqual(metrics["fact_rows"], 2)
        self.assertEqual(metrics["null_price_count"], 1)
        self.assertEqual(metrics["invalid_price_count"], 1)
        self.assertEqual(metrics["null_area_count"], 1)
        self.assertEqual(metrics["invalid_area_count"], 1)
        self.assertEqual(metrics["duplicate_non_empty_detail_url_rows"], 2)

    def test_enforce_quality_gate_fail(self) -> None:
        metrics = {
            "null_price_ratio": 0.12,
            "invalid_price_ratio": 0.03,
            "null_area_ratio": 0.20,
            "invalid_area_ratio": 0.0,
            "duplicate_non_empty_detail_url_ratio": 0.0,
            "fact_row_ratio": 0.75,
        }
        thresholds = QualityThresholds(
            max_null_price_ratio=0.10,
            max_invalid_price_ratio=0.02,
            max_null_area_ratio=0.35,
            max_invalid_area_ratio=0.01,
            max_duplicate_detail_url_ratio=0.01,
            min_fact_row_ratio=0.60,
        )

        with self.assertRaises(QualityGateError):
            enforce_quality_gate(metrics, thresholds)

    def test_enforce_quality_gate_pass(self) -> None:
        metrics = {
            "null_price_ratio": 0.03,
            "invalid_price_ratio": 0.005,
            "null_area_ratio": 0.25,
            "invalid_area_ratio": 0.0,
            "duplicate_non_empty_detail_url_ratio": 0.0,
            "fact_row_ratio": 0.75,
        }
        thresholds = QualityThresholds(
            max_null_price_ratio=0.10,
            max_invalid_price_ratio=0.02,
            max_null_area_ratio=0.35,
            max_invalid_area_ratio=0.01,
            max_duplicate_detail_url_ratio=0.01,
            min_fact_row_ratio=0.60,
        )

        enforce_quality_gate(metrics, thresholds)


if __name__ == "__main__":
    unittest.main()
