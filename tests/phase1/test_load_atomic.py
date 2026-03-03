import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from etl.phase1.load import _atomic_replace_dataframe


class AtomicLoadTests(unittest.TestCase):
    def test_atomic_replace_sequence(self) -> None:
        df = pd.DataFrame({"id": ["1"], "value": ["x"]})
        engine = MagicMock()
        conn = MagicMock()

        begin_cm = MagicMock()
        begin_cm.__enter__.return_value = conn
        begin_cm.__exit__.return_value = False
        engine.begin.return_value = begin_cm

        fake_uuid = types.SimpleNamespace(hex="abcdef123456")

        with patch("etl.phase1.load.uuid.uuid4", return_value=fake_uuid), patch.object(
            pd.DataFrame, "to_sql", autospec=True
        ) as to_sql_mock:
            _atomic_replace_dataframe(
                engine=engine,
                schema="staging",
                table="stg_listings_raw",
                df=df,
                dtype={"id": object(), "value": object()},
            )

            to_sql_mock.assert_called_once()
            sql_calls = [call.args[0] for call in conn.exec_driver_sql.call_args_list]
            self.assertEqual(len(sql_calls), 3)
            self.assertIn("TRUNCATE TABLE staging.stg_listings_raw", sql_calls[0])
            self.assertIn("INSERT INTO staging.stg_listings_raw SELECT * FROM staging.__tmp_stg_listings_raw_abcdef12", sql_calls[1])
            self.assertIn("DROP TABLE staging.__tmp_stg_listings_raw_abcdef12", sql_calls[2])


if __name__ == "__main__":
    unittest.main()
