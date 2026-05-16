import asyncio
import json
from urllib.parse import urlparse

from binance_funding.config import proxy
from binance_funding.utils.log_kit import logger

USDT_FUTURES_STREAM_URL = "wss://fstream.binance.com/stream?streams="


def mark_price_stream_path(symbols: list[str]) -> str:
    return "/".join(f"{symbol.lower()}@markPrice@1s" for symbol in symbols)


class MarkPriceWebsocket:
    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.url = USDT_FUTURES_STREAM_URL + mark_price_stream_path(symbols)

    def _connection_kwargs(self) -> dict:
        kwargs = {"close_timeout": 0.1, "ping_interval": 20, "ping_timeout": 20}
        if proxy:
            parsed = urlparse(proxy)
            if parsed.scheme in ("http", "https", "socks5"):
                kwargs["proxy"] = proxy
            else:
                logger.warning(f"Unsupported proxy scheme for websocket: {parsed.scheme}")
        return kwargs

    async def listen(self):
        import websockets

        while True:
            try:
                async with websockets.connect(self.url, **self._connection_kwargs()) as ws:
                    logger.info(f"WS connected for {len(self.symbols)} symbols")
                    async for raw in ws:
                        try:
                            yield json.loads(raw)
                        except json.JSONDecodeError:
                            logger.debug(f"Invalid websocket JSON: {raw[:120]}")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"WS disconnected ({exc}); reconnecting in 5s")
                await asyncio.sleep(5)
