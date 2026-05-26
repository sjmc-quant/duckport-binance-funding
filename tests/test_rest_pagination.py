import unittest
from unittest.mock import patch

import pandas as pd

from binance_funding import data_jobs
from binance_funding.data_jobs import FundingFetcher


class FakeApi:
    def __init__(self):
        self.calls = []

    async def funding_rate_history(self, **params):
        self.calls.append(params)
        if len(self.calls) == 1:
            return [
                {
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.1",
                    "fundingTime": 0,
                    "markPrice": "100",
                }
                for _ in range(600)
            ] + [
                {
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.2",
                    "fundingTime": 3_600_008,
                    "markPrice": "101",
                }
                for _ in range(399)
            ] + [
                {
                    "symbol": "BTCUSDC",
                    "fundingRate": "0.2",
                    "fundingTime": 3_600_008,
                    "markPrice": "101",
                }
            ]
        if params["startTime"] == 3_600_008 and params["endTime"] == 3_600_008:
            return [
                {
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.2",
                    "fundingTime": 3_600_008,
                    "markPrice": "101",
                }
                for _ in range(600)
            ]
        return [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.3",
                "fundingTime": 7_200_002,
                "markPrice": "102",
            }
        ]


class FailingOnceApi:
    def __init__(self):
        self.calls = 0

    async def funding_rate_history(self, **params):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary failure")
        return [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.1",
                "fundingTime": 0,
                "markPrice": "100",
            }
        ]


class RestPaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetcher_paginates_until_short_page(self):
        api = FakeApi()
        fetcher = FundingFetcher(api)
        frame = await fetcher.funding_history(
            pd.Timestamp(0, unit="ms", tz="UTC"),
            pd.Timestamp(7_200_003, unit="ms", tz="UTC"),
        )
        self.assertEqual(len(api.calls), 3)
        self.assertEqual(fetcher.funding_rate_requests, 3)
        self.assertNotIn("symbol", api.calls[0])
        self.assertEqual(api.calls[0]["limit"], 1000)
        self.assertEqual(api.calls[1]["startTime"], 3_600_008)
        self.assertEqual(api.calls[1]["endTime"], 3_600_008)
        self.assertEqual(api.calls[2]["startTime"], 3_600_009)
        self.assertEqual(len(frame), 3)
        self.assertEqual(list(frame["funding_time"]), [
            pd.Timestamp(0, unit="ms", tz="UTC"),
            pd.Timestamp(3_600_000, unit="ms", tz="UTC"),
            pd.Timestamp(7_200_000, unit="ms", tz="UTC"),
        ])
        self.assertAlmostEqual(frame["mark_price"].iloc[-1], 102.0)

    async def test_failed_request_counts_attempts(self):
        api = FailingOnceApi()
        fetcher = FundingFetcher(api)
        with patch.object(data_jobs, "FUNDING_RETRY_SLEEP_SECONDS", 0):
            frame = await fetcher.funding_history(
                pd.Timestamp(0, unit="ms", tz="UTC"),
                pd.Timestamp(1, unit="ms", tz="UTC"),
            )
        self.assertEqual(fetcher.funding_rate_requests, 2)
        self.assertEqual(len(frame), 1)


if __name__ == "__main__":
    unittest.main()
