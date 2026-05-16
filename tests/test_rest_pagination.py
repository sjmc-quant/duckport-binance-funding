import unittest

import pandas as pd

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
                    "fundingTime": i,
                    "markPrice": "100",
                }
                for i in range(1000)
            ]
        return [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.2",
                "fundingTime": 1001,
                "markPrice": "101",
            }
        ]


class RestPaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetcher_paginates_until_short_page(self):
        api = FakeApi()
        fetcher = FundingFetcher(api)
        frame = await fetcher.funding_history_for_symbol(
            "BTCUSDT",
            pd.Timestamp(0, unit="ms", tz="UTC"),
            pd.Timestamp(2000, unit="ms", tz="UTC"),
        )
        self.assertEqual(len(api.calls), 2)
        self.assertEqual(api.calls[1]["startTime"], 1000)
        self.assertEqual(len(frame), 1001)
        self.assertAlmostEqual(frame["mark_price"].iloc[-1], 101.0)


if __name__ == "__main__":
    unittest.main()
