import unittest

from binance_funding.api.ws import ALL_MARKET_MARK_PRICE_STREAM, MarkPriceWebsocket


class WsUrlTests(unittest.TestCase):
    def test_uses_all_market_mark_price_stream(self):
        socket = MarkPriceWebsocket()
        self.assertIn("/market/stream?streams=", socket.url)
        self.assertTrue(socket.url.endswith(ALL_MARKET_MARK_PRICE_STREAM))


if __name__ == "__main__":
    unittest.main()
