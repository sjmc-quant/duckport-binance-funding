import unittest
from datetime import datetime, timezone

import pandas as pd

from binance_funding.config import (
    FUNDING_POLL_DELAY_SECONDS,
    FUNDING_REQUEST_INTERVAL_SECONDS,
    FUNDING_RETRY_SLEEP_SECONDS,
)
from binance_funding.data_jobs import FundingDataJobs, current_hour_watermark


class FakeClient:
    def __init__(self, duck_time):
        self.duck_time = duck_time
        self.replaced_duck_times = []

    def read_duck_time(self):
        return self.duck_time

    def replace_duck_time(self, duck_time_str):
        self.replaced_duck_times.append(duck_time_str)


class RestPollingTests(unittest.TestCase):
    def test_default_poll_delay_is_five_seconds(self):
        self.assertEqual(FUNDING_POLL_DELAY_SECONDS, 5)

    def test_default_funding_request_timing(self):
        self.assertEqual(FUNDING_REQUEST_INTERVAL_SECONDS, 0.2)
        self.assertEqual(FUNDING_RETRY_SLEEP_SECONDS, 75)

    def test_next_poll_time_uses_next_hour_plus_delay(self):
        now = datetime(2026, 1, 1, 7, 3, 10, tzinfo=timezone.utc)
        next_time = FundingDataJobs.next_poll_time(now, 5)
        self.assertEqual(next_time, datetime(2026, 1, 1, 8, 0, 5, tzinfo=timezone.utc))

    def test_next_poll_time_from_exact_hour_waits_next_hour(self):
        now = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        next_time = FundingDataJobs.next_poll_time(now, 30)
        self.assertEqual(next_time, datetime(2026, 1, 1, 9, 0, 30, tzinfo=timezone.utc))

    def test_query_start_time_continues_after_watermark(self):
        job = FundingDataJobs(FakeClient(pd.Timestamp("2026-01-01 08:00:00Z")), auto_start=False)
        self.assertEqual(job._query_start_time(), pd.Timestamp("2026-01-01 08:00:01Z"))

    def test_current_hour_watermark_uses_hour_floor(self):
        self.assertEqual(
            current_hour_watermark(pd.Timestamp("2026-01-01 08:12:34Z")),
            pd.Timestamp("2026-01-01 08:00:00Z"),
        )


if __name__ == "__main__":
    unittest.main()
