import unittest

import pandas as pd

from binance_funding.component.funding import (
    parse_rest_funding_history,
)


class FundingParserTests(unittest.TestCase):
    def test_parse_rest_funding_history(self):
        frame = parse_rest_funding_history(
            [
                {
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.00010000",
                    "fundingTime": 1570636800008,
                    "markPrice": "34287.54619963",
                },
                {
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.00020000",
                    "fundingTime": 1570636800024,
                    "markPrice": "34288.00000000",
                }
            ]
        )
        self.assertEqual(list(frame.columns), [
            "funding_time",
            "symbol",
            "funding_rate",
            "mark_price",
        ])
        self.assertEqual(frame.loc[0, "symbol"], "BTCUSDT")
        self.assertEqual(len(frame), 1)
        self.assertAlmostEqual(frame.loc[0, "funding_rate"], 0.0002)
        self.assertAlmostEqual(frame.loc[0, "mark_price"], 34288.0)
        self.assertEqual(frame.loc[0, "funding_time"], pd.Timestamp("2019-10-09 16:00:00Z"))

if __name__ == "__main__":
    unittest.main()
