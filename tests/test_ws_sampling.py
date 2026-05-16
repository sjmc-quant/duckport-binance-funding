import unittest

import pandas as pd

from binance_funding.data_jobs import FundingDataJobs


class WsSamplingTests(unittest.TestCase):
    def test_samples_only_final_minute_before_settlement(self):
        base = {
            "symbol": "BTCUSDT",
            "mark_price": 100.0,
            "last_funding_rate": 0.001,
            "next_funding_time": pd.Timestamp("2026-01-01 08:00:00Z"),
        }

        within = {**base, "event_time": pd.Timestamp("2026-01-01 07:59:00Z")}
        before_window = {**base, "event_time": pd.Timestamp("2026-01-01 07:58:59Z")}
        after_settlement = {**base, "event_time": pd.Timestamp("2026-01-01 08:00:01Z")}

        self.assertTrue(FundingDataJobs._should_sample_ws_row(within))
        self.assertFalse(FundingDataJobs._should_sample_ws_row(before_window))
        self.assertFalse(FundingDataJobs._should_sample_ws_row(after_settlement))


if __name__ == "__main__":
    unittest.main()
