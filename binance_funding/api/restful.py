import aiohttp

from binance_funding.api.exceptions import BinanceAPIException, BinanceRequestException
from binance_funding.config import proxy


class BinanceFundingRestApi:
    PREFIX = "https://fapi.binance.com/fapi"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _handle_response(self, response: aiohttp.ClientResponse):
        body = await response.text()
        if not str(response.status).startswith("2"):
            raise BinanceAPIException(response.status, body)
        try:
            return await response.json()
        except ValueError as exc:
            raise BinanceRequestException(f"Invalid JSON response: {body}") from exc

    async def _get(self, path: str, params: dict | None = None):
        async with self.session.get(f"{self.PREFIX}{path}", params=params or {}, proxy=proxy) as resp:
            return await self._handle_response(resp)

    async def time(self) -> dict:
        return await self._get("/v1/time")

    async def exchange_info(self) -> dict:
        return await self._get("/v1/exchangeInfo")

    async def funding_rate_history(self, **params) -> list:
        return await self._get("/v1/fundingRate", params)

    async def premium_index(self, symbol: str | None = None):
        params = {"symbol": symbol} if symbol else {}
        return await self._get("/v1/premiumIndex", params)
