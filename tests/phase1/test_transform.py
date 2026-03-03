import unittest

import pandas as pd

from etl.phase1.transform import build_fact_dataframe, extract_location


class TransformTests(unittest.TestCase):
    def test_extract_location_normalization(self) -> None:
        province, district = extract_location("Q 12, TP HCM")
        self.assertEqual(province, "Ho Chi Minh")
        self.assertEqual(district, "Quan 12")

        province2, district2 = extract_location("Đống Đa, Hà Nội")
        self.assertEqual(province2, "Ha Noi")
        self.assertEqual(district2, "Dong Da")

    def test_build_fact_dataframe_dedup_and_price_per_m2(self) -> None:
        staging_df = pd.DataFrame(
            [
                {
                    "id": "1",
                    "detail_url": "url-a",
                    "title": "A old",
                    "location": "Q 1, TP HCM",
                    "timeline_hours": 5,
                    "area_m2": 50.0,
                    "bedrooms": 2,
                    "bathrooms": 2,
                    "floors": 2,
                    "frontage": 1.0,
                    "price_million_vnd": 5000.0,
                },
                {
                    "id": "1",
                    "detail_url": "url-a",
                    "title": "A new",
                    "location": "Q 1, TP HCM",
                    "timeline_hours": 2,
                    "area_m2": 50.0,
                    "bedrooms": 3,
                    "bathrooms": 2,
                    "floors": 2,
                    "frontage": 1.0,
                    "price_million_vnd": 6000.0,
                },
                {
                    "id": "2",
                    "detail_url": "",
                    "title": "No URL",
                    "location": "Dong Da, Ha Noi",
                    "timeline_hours": 3,
                    "area_m2": 40.0,
                    "bedrooms": 2,
                    "bathrooms": 1,
                    "floors": 1,
                    "frontage": 0.0,
                    "price_million_vnd": 4000.0,
                },
                {
                    "id": "3",
                    "detail_url": "url-b",
                    "title": "Bad price",
                    "location": "Q 3, TP HCM",
                    "timeline_hours": 1,
                    "area_m2": 30.0,
                    "bedrooms": 1,
                    "bathrooms": 1,
                    "floors": 1,
                    "frontage": 0.0,
                    "price_million_vnd": 0.0,
                },
                {
                    "id": "4",
                    "detail_url": "url-c",
                    "title": "Missing location",
                    "location": "",
                    "timeline_hours": 1,
                    "area_m2": 20.0,
                    "bedrooms": 1,
                    "bathrooms": 1,
                    "floors": 1,
                    "frontage": 0.0,
                    "price_million_vnd": 2000.0,
                },
            ]
        )

        fact_df = build_fact_dataframe(staging_df)

        # One row dropped by invalid price, one row deduped by detail_url.
        self.assertEqual(len(fact_df), 3)

        kept = fact_df[fact_df["listing_id"] == "1"].iloc[0]
        self.assertEqual(kept["title"], "A new")
        self.assertAlmostEqual(float(kept["price_per_m2"]), 120.0)

        missing_location = fact_df[fact_df["listing_id"] == "4"].iloc[0]
        self.assertEqual(missing_location["province"], "Unknown")
        self.assertEqual(missing_location["district"], "Unknown")

    def test_build_fact_dataframe_raises_on_listing_id_conflict(self) -> None:
        staging_df = pd.DataFrame(
            [
                {
                    "id": "100",
                    "detail_url": "url-a",
                    "title": "A",
                    "location": "Q 1, TP HCM",
                    "timeline_hours": 1,
                    "area_m2": 20.0,
                    "bedrooms": 1,
                    "bathrooms": 1,
                    "floors": 1,
                    "frontage": 1.0,
                    "price_million_vnd": 2000.0,
                },
                {
                    "id": "100",
                    "detail_url": "url-b",
                    "title": "B",
                    "location": "Q 1, TP HCM",
                    "timeline_hours": 2,
                    "area_m2": 25.0,
                    "bedrooms": 2,
                    "bathrooms": 1,
                    "floors": 1,
                    "frontage": 1.0,
                    "price_million_vnd": 2500.0,
                },
            ]
        )

        with self.assertRaisesRegex(ValueError, "Duplicate listing_id values remain"):
            build_fact_dataframe(staging_df)


if __name__ == "__main__":
    unittest.main()
