import unittest
from datetime import date

import pandas as pd

from binance_funding.data_jobs import iter_month_windows


class HistoryWindowTests(unittest.TestCase):
    def test_iter_month_windows_starts_from_exact_start_time(self):
        windows = iter_month_windows(
            date(2024, 1, 15),
            pd.Timestamp("2024-03-03 12:00:00Z"),
        )
        self.assertEqual(windows, [
            (pd.Timestamp("2024-01-15 00:00:00Z"), pd.Timestamp("2024-02-01 00:00:00Z")),
            (pd.Timestamp("2024-02-01 00:00:00Z"), pd.Timestamp("2024-03-01 00:00:00Z")),
            (pd.Timestamp("2024-03-01 00:00:00Z"), pd.Timestamp("2024-03-03 12:00:00Z")),
        ])

    def test_iter_month_windows_returns_empty_for_future_start(self):
        windows = iter_month_windows(
            date(2024, 4, 1),
            pd.Timestamp("2024-03-03 12:00:00Z"),
        )
        self.assertEqual(windows, [])


if __name__ == "__main__":
    unittest.main()
