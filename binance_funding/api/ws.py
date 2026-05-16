import asyncio
import json
from urllib.parse import urlparse

from binance_funding.config import proxy
from binance_funding.utils.log_kit import logger

USDT_FUTURES_MARKET_STREAM_URL = "wss://fstream.binance.com/market/stream?streams="
ALL_MARKET_MARK_PRICE_STREAM = "!markPrice@arr@1s"
WS_RECV_TIMEOUT_SECONDS = 10


class MarkPriceWebsocketError(Exception):
    pass


class MarkPriceWebsocket:
    def __init__(self):
        self.url = USDT_FUTURES_MARKET_STREAM_URL + ALL_MARKET_MARK_PRICE_STREAM

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

        try:
            async with websockets.connect(self.url, **self._connection_kwargs()) as ws:
                logger.info(f"WS connected: {ALL_MARKET_MARK_PRICE_STREAM}")
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT_SECONDS)
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError:
                        logger.debug(f"Invalid websocket JSON: {raw[:120]}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise MarkPriceWebsocketError(f"all-market mark-price stream failed: {exc}") from exc
