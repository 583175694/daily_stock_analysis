# -*- coding: utf-8 -*-
import unittest
from datetime import date

import pandas as pd

from src.storage import DatabaseManager
from src.stock_picker.repository import StockPickerRepository


class TestStockPickerRepository(unittest.TestCase):
    def setUp(self) -> None:
        DatabaseManager.reset_instance()
        self.db = DatabaseManager(db_url="sqlite:///:memory:")
        self.repo = StockPickerRepository(db_manager=self.db)

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()

    def test_get_recent_daily_rows_returns_detached_safe_snapshots(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "date": date(2026, 4, 10),
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "volume": 1000.0,
                    "amount": 10200.0,
                    "pct_chg": 2.0,
                    "ma5": 10.0,
                    "ma10": 9.9,
                    "ma20": 9.7,
                    "volume_ratio": 1.2,
                },
                {
                    "date": date(2026, 4, 11),
                    "open": 10.2,
                    "high": 10.9,
                    "low": 10.1,
                    "close": 10.8,
                    "volume": 1200.0,
                    "amount": 12960.0,
                    "pct_chg": 5.88,
                    "ma5": 10.1,
                    "ma10": 10.0,
                    "ma20": 9.8,
                    "volume_ratio": 1.35,
                },
            ]
        )
        self.db.save_daily_data(frame, code="600519", data_source="unit-test")

        rows = self.repo.get_recent_daily_rows("600519", limit=5)

        self.assertEqual(len(rows), 2)
        self.assertIsInstance(rows[0], dict)
        self.assertEqual(rows[0]["date"], date(2026, 4, 11))
        self.assertEqual(rows[0]["close"], 10.8)
        self.assertEqual(rows[1]["date"], date(2026, 4, 10))


if __name__ == "__main__":
    unittest.main()
