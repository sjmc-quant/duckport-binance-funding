import tempfile
import unittest
from pathlib import Path

import pandas as pd

from binance_funding.component.funding import (
    parse_funding_csv,
    parse_mark_price_event,
    parse_rest_funding_history,
    usdt_perpetual_symbols,
    ws_funding_frame,
)


class FundingParserTests(unittest.TestCase):
    def test_parse_funding_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "BTCUSDT-fundingRate-2026-04.csv"
            path.write_text(
                "calc_time,funding_interval_hours,last_funding_rate\n"
                "1775001600000,8,-0.00003449\n",
                encoding="utf-8",
            )
            frame = parse_funding_csv(str(path))

        self.assertEqual(list(frame.columns), [
            "funding_time",
            "symbol",
            "funding_rate",
            "mark_price",
            "funding_interval_hours",
            "source",
        ])
        self.assertEqual(frame.loc[0, "symbol"], "BTCUSDT")
        self.assertEqual(frame.loc[0, "funding_interval_hours"], 8)
        self.assertEqual(frame.loc[0, "source"], "hist")
        self.assertTrue(pd.isna(frame.loc[0, "mark_price"]))

    def test_parse_rest_funding_history(self):
        frame = parse_rest_funding_history(
            [
                {
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.00010000",
                    "fundingTime": 1570636800000,
                    "markPrice": "34287.54619963",
                }
            ],
            interval_hours=4,
        )
        self.assertEqual(frame.loc[0, "funding_interval_hours"], 4)
        self.assertAlmostEqual(frame.loc[0, "mark_price"], 34287.54619963)
        self.assertEqual(frame.loc[0, "source"], "rest")

    def test_parse_mark_price_event(self):
        row = parse_mark_price_event(
            {
                "data": {
                    "e": "markPriceUpdate",
                    "E": 1562305380000,
                    "s": "BTCUSDT",
                    "p": "11794.15000000",
                    "i": "11784.62659091",
                    "P": "11784.25641265",
                    "r": "0.00038167",
                    "T": 1562306400000,
                }
            }
        )
        self.assertEqual(row["symbol"], "BTCUSDT")
        self.assertAlmostEqual(row["last_funding_rate"], 0.00038167)
        frame = ws_funding_frame([row])
        self.assertEqual(frame.loc[0, "symbol"], "BTCUSDT")
        self.assertEqual(frame.loc[0, "funding_time"], row["next_funding_time"])
        self.assertAlmostEqual(frame.loc[0, "funding_rate"], 0.00038167)
        self.assertEqual(frame.loc[0, "source"], "ws")

    def test_usdt_perpetual_symbols(self):
        symbols = usdt_perpetual_symbols(
            {
                "symbols": [
                    {"symbol": "BTCUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                    {"symbol": "BTCUSDC", "quoteAsset": "USDC", "contractType": "PERPETUAL", "status": "TRADING"},
                    {"symbol": "ETHUSDT_240628", "quoteAsset": "USDT", "contractType": "CURRENT_QUARTER", "status": "TRADING"},
                    {"symbol": "OLDUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "BREAK"},
                ]
            }
        )
        self.assertEqual(symbols, ["BTCUSDT"])


if __name__ == "__main__":
    unittest.main()
